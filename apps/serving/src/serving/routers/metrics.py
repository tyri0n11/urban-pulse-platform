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
    "Z-score values (z_avg, z_max) are DIMENSIONLESS — they have NO units. "
    "z_avg=28.3 means 28.3 standard deviations, NOT 28.3 degrees Celsius. "
    "NEVER confuse Z-score values with weather measurements (temperature, rain, wind). "
    "Weather data (°C, mm, km/h) is completely separate from Z-score data (σ). "
    "Z-score DIRECTIONALITY — MANDATORY: "
    "z > 0 means heavy_ratio is ABOVE the route's historical baseline → heavier than usual → potential congestion. "
    "z < 0 means heavy_ratio is BELOW the route's historical baseline → lighter than usual → unusually free traffic. "
    "NEVER describe a negative Z-score as congestion, slow traffic, or tắc nghẽn. "
    "Negative Z-score flagged by IsolationForest means the route is anomalously quiet — possible causes: rerouting, road closure, late-night low demand, or data sparsity. "
    "Z-score threshold is ONE-SIDED (only z > threshold triggers z_flagged); negative-Z routes appear ONLY because IsolationForest (bidirectional) flagged them. "
    "IsolationForest (IF) is a BINARY flag — it is either 'flagged' or 'not flagged', never a number. "
    "if_flagged=M/N means M out of N observation windows were flagged by IsolationForest. "
    "z_flagged=M/N means M out of N windows exceeded the Z-score threshold. "
    "When N is small (n=1 or n=2), treat the flagging as low-confidence — do NOT conclude a strong pattern from a single observation. "
    "A pattern is only reliable when N≥5 and the flagged fraction is high (e.g. if_flagged=4/5). "
    "NEVER write 'IF: <number>' or assign any numerical value to IF. "
    "When referencing IF, write 'IF flagged', 'IF anomaly detected', or 'both signals confirmed' — nothing else. "
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

_SECTIONS_MULTIDAY = {
    "vi": [
        "1. **Mẫu lặp lại đa tín hiệu** — nhóm các bất thường theo ngày trong tuần và khung giờ (không phải ngày cụ thể). Ví dụ: 'Thứ 6–7 lúc 17:00–19:00 thường xuyên có cả hai tín hiệu xác nhận trên Tuyến X'. Bỏ qua sự kiện đơn lẻ không lặp lại.",
        "2. **Nguyên nhân gốc rễ** — giải thích tại sao mẫu ngày/giờ đó lặp lại, liên kết với đặc điểm giao thông TP.HCM (peak sáng/chiều, KCN, cảng, cuối tuần vs ngày thường, đặc trưng từng zone).",
        "3. **Tín hiệu đơn lẻ theo xu hướng** — các tuyến chỉ có Z-score hoặc chỉ có IF flagged: mô tả xu hướng theo ngày trong tuần, không phải ngày cụ thể. Dùng Z-score (số) để định lượng, IF chỉ là 'flagged/không flagged'.",
        "4. **Khuyến nghị theo lịch tuần** — mỗi khuyến nghị PHẢI gắn với ngày trong tuần + khung giờ (UTC+7) dựa trên mẫu lặp lại (VD: 'Thứ 2–6, 07:00–09:00, tuyến X'). Ưu tiên các mẫu xuất hiện ≥3 lần trong window.",
    ],
    "en": [
        "1. **Recurring dual-signal patterns** — group anomalies by day-of-week and hour range, NOT by specific dates. Example: 'Fri–Sat 17:00–19:00 consistently shows both signals on Route X'. Ignore one-off incidents that do not repeat.",
        "2. **Root causes** — explain WHY that day-of-week / hour pattern recurs, linking to HCMC traffic characteristics (morning/evening peak, industrial zones, port logistics, weekday vs weekend, zone-specific traits).",
        "3. **Single-signal trends** — for Z-score-only or IF-only routes, describe the day-of-week trend, not specific dates. Quantify with Z-score values; describe IF as 'flagged' or 'not flagged' only.",
        "4. **Weekly schedule recommendations** — every recommendation MUST specify day-of-week + hour range (UTC+7) derived from recurring patterns (e.g. 'Mon–Fri, 07:00–09:00, Route X'). Prioritise patterns appearing ≥3 times within the window.",
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
    context: str = Field(..., min_length=10, max_length=12000)
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
    span_h: int | None = None
    try:
        if window_from and window_to:
            frm = datetime.fromisoformat(window_from).astimezone(_HCMC_TZ)
            to_ = datetime.fromisoformat(window_to).astimezone(_HCMC_TZ)
            span_h = int((to_ - frm).total_seconds() / 3600)
    except Exception:
        pass

    multi_day = span_h is not None and span_h > 24

    if multi_day:
        sections = "\n".join(_SECTIONS_MULTIDAY.get(lang, _SECTIONS_MULTIDAY["en"]))
        concise = (
            "3–4 câu mỗi mục. Không chào hỏi. "
            "Ưu tiên MẪU LẶP LẠI (ví dụ: 'mỗi thứ 7 lúc 17h') hơn sự kiện đơn lẻ. "
            "Mục 4 phải gắn khuyến nghị với ngày trong tuần + khung giờ (UTC+7), không phải ngày cụ thể."
            if lang == "vi"
            else "3–4 sentences per section. No greetings. "
            "Prioritise RECURRING PATTERNS (e.g. 'every Saturday at 17:00') over isolated incidents. "
            "Section 4 must anchor recommendations to day-of-week + hour range (UTC+7), not specific dates."
        )
    else:
        sections = "\n".join(_SECTIONS.get(lang, _SECTIONS["en"]))
        concise = (
            "3–4 câu mỗi mục. Không chào hỏi. Mục 4 phải có giờ cụ thể (UTC+7) cho mỗi khuyến nghị."
            if lang == "vi"
            else "3–4 sentences per section. No greetings. Section 4 must include an explicit hour range (UTC+7) for every recommendation."
        )

    time_header = _format_window_header(lang, window_from, window_to)
    parts = [
        time_header,
        f"Provide analysis in 4 sections:\n{sections}\n{concise}",
        f"=== HEATMAP DATA ===\n{context}",
    ]
    if external:
        parts.append(external)
    return "\n\n".join(parts)


def _aggregate_multiday_context(
    rows: list[dict[str, Any]],
) -> tuple[str, list[tuple[str, int, int]]]:
    """Aggregate raw heatmap rows into a compact (route, day_of_week, hour) summary.

    Returns (text, top_anomaly_slots) where top_anomaly_slots is a list of
    (route_id, dow, hour) for the most-flagged slots — used to fetch targeted weather context.
    """
    from collections import defaultdict

    buckets: dict[tuple[str, int, int], dict[str, Any]] = defaultdict(
        lambda: {"zscores": [], "anomaly": 0, "iforest": 0, "both": 0, "n": 0}
    )

    for r in rows:
        ws = r.get("window_start")
        if ws is None:
            continue
        if isinstance(ws, str):
            ws = datetime.fromisoformat(ws)
        ws_local = ws.astimezone(_HCMC_TZ)
        key = (r["route_id"], ws_local.weekday(), ws_local.hour)
        b = buckets[key]
        b["n"] += 1
        if r.get("duration_zscore") is not None:
            b["zscores"].append(float(r["duration_zscore"]))
        if r.get("is_anomaly"):
            b["anomaly"] += 1
        if r.get("iforest_anomaly"):
            b["iforest"] += 1
        if r.get("both_anomaly"):
            b["both"] += 1

    route_blocks: dict[str, list[str]] = defaultdict(list)
    dow_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

    # Track anomaly score per slot for top-N selection (both_flagged > iforest > zscore)
    slot_scores: list[tuple[float, tuple[str, int, int]]] = []

    for (route_id, dow, hour), b in sorted(buckets.items(), key=lambda x: (x[0][0], x[0][1], x[0][2])):
        n = b["n"]
        if n == 0:
            continue
        zs = b["zscores"]
        z_avg = round(sum(zs) / len(zs), 2) if zs else None
        z_max = round(max(zs), 2) if zs else None
        ar = round(b["anomaly"] / n, 2)
        ir = round(b["iforest"] / n, 2)
        if ar == 0 and ir == 0 and (z_avg is None or abs(z_avg) < 1.0):
            continue
        n_z = b["anomaly"]
        n_if = b["iforest"]
        n_both = b["both"]
        dow_name = dow_names[dow]
        parts = [f"{dow_name} {hour:02d}:00"]
        if z_avg is not None:
            parts.append(f"z_avg={z_avg}σ z_max={z_max}σ")
        if n_z > 0:
            parts.append(f"z_flagged={n_z}/{n}")
        if n_if > 0:
            parts.append(f"if_flagged={n_if}/{n}")
        if n_both > 0:
            parts.append(f"both_flagged={n_both}/{n}")
        parts.append(f"n={n}")
        route_blocks[route_id].append("  " + " | ".join(parts))

        # Score = both weight + iforest rate; only meaningful slots (n≥2) counted for weather fetch
        if n >= 2:
            score = (b["both"] / n) * 2 + (b["iforest"] / n)
            if score > 0:
                slot_scores.append((score, (route_id, dow, hour)))

    legend = (
        "Legend: z_avg/z_max=Z-score mean/max in σ units (dimensionless, NOT temperature/weather) "
        "| z_flagged=flagged/total windows where Z-score exceeded threshold "
        "| if_flagged=flagged/total windows IsolationForest flagged "
        "| both_flagged=flagged/total windows BOTH signals simultaneously active "
        "| n=total observation window count (low n → low statistical confidence)"
    )
    lines: list[str] = [legend, ""]
    for route_id, entries in sorted(route_blocks.items()):
        label = route_id.replace("_to_", " → ").replace("_", " ").title()
        lines.append(label)
        lines.extend(entries)

    top_slots = [slot for _, slot in sorted(slot_scores, reverse=True)[:3]]
    return "\n".join(lines), top_slots



@router.post("/analyze")
async def heatmap_analyze(
    req: HeatmapAnalyzeRequest,
    conn: asyncpg.Connection = Depends(get_db),
) -> StreamingResponse:
    """Stream LLM analysis of heatmap data. All prompt engineering is server-side."""
    context = req.context
    span_h: int | None = None
    try:
        if req.window_from and req.window_to:
            frm = datetime.fromisoformat(req.window_from).astimezone(_HCMC_TZ)
            to_ = datetime.fromisoformat(req.window_to).astimezone(_HCMC_TZ)
            span_h = int((to_ - frm).total_seconds() / 3600)
    except Exception:
        pass

    if span_h is not None and span_h > 24:
        rows: list[dict[str, Any]] = []
        try:
            frm_dt = datetime.fromisoformat(req.window_from)  # type: ignore[arg-type]
            to_dt = datetime.fromisoformat(req.window_to)  # type: ignore[arg-type]
            rows = await metrics_repo.fetch_heatmap_range(conn, frm_dt, to_dt)
            context, top_slots = _aggregate_multiday_context(rows)
        except Exception:
            top_slots = []
            pass  # fall back to req.context if fetch fails

        external = "" if not top_slots else await fetch_heatmap_external_context(req.route_ids, None)
    else:
        weather = await fetch_current_weather()
        external = await fetch_heatmap_external_context(req.route_ids, weather)

    lang_note = _LANG_INSTRUCTIONS.get(req.lang, _LANG_INSTRUCTIONS["en"])
    system = f"{_ANALYZE_SYSTEM_BASE} {lang_note}"
    user_prompt = _build_analyze_prompt(
        context, req.lang, external, req.window_from, req.window_to
    )
    return StreamingResponse(
        stream_ollama(system, user_prompt, temperature=0.0),
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
