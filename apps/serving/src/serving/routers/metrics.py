"""Router: traffic metrics endpoints."""

import json
import logging
import re
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
from serving.services.multiday_analysis_service import preprocess, fetch_rag_for_multiday, run_multiday_chain
from serving.utils.weather import fetch_current_weather

logger = logging.getLogger(__name__)
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
    "You are a traffic operations analyst for Ho Chi Minh City (HCMC), Vietnam. "
    "Write a structured traffic report for urban traffic managers based solely on the heatmap data provided. "
    "Rules: "
    "(1) Use ONLY data provided — never invent routes, hours, or numbers. "
    "(2) Routes are 'Zone X → Zone Y' — write 'Zone', never 'Zona'. "
    "(3) z_avg and z_max are Z-scores in standard deviations (σ) — dimensionless, NOT temperature or speed. "
    "(4) z > 0 = heavier traffic than baseline (congestion risk). z < 0 = quieter than baseline (unusual low demand). "
    "(5) if_flagged is an IsolationForest TRAFFIC ANOMALY flag — it is NOT a data-quality flag. Never say 'samples need review'. "
    "(6) both_flagged = both Z-score AND IsolationForest confirmed simultaneously — highest confidence. "
    "(7) Write 'IF flagged' or 'IF not flagged' only — never assign IF a number. "
    "(8) Follow exactly the 4-section structure and language specified in the user prompt."
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

_SIGNAL_CHECKLIST = {
    "vi": (
        "=== HƯỚNG DẪN ĐỌC DỮ LIỆU (đọc trước khi phân tích) ===\n"
        "• z_avg / z_max: Z-score đo bằng σ (không đơn vị) — KHÔNG phải nhiệt độ hay tốc độ.\n"
        "  z > 0 = lưu lượng nặng hơn baseline → nguy cơ tắc nghẽn.\n"
        "  z < 0 = lưu lượng nhẹ hơn baseline → bất thường yên tĩnh.\n"
        "• if_flagged M/N: IsolationForest phát hiện BẤT THƯỜNG GIAO THÔNG trong M/N cửa sổ. KHÔNG phải lỗi dữ liệu.\n"
        "• both_flagged M/N: CẢ HAI tín hiệu xác nhận — độ tin cậy cao nhất.\n"
        "• Chỉ viết 'IF flagged' hoặc 'IF không flagged' — tuyệt đối không gán số cho IF."
    ),
    "en": (
        "=== SIGNAL GUIDE (read before analysis) ===\n"
        "• z_avg / z_max: Z-score in σ (dimensionless) — NOT temperature or speed.\n"
        "  z > 0 = heavier than baseline → congestion risk.\n"
        "  z < 0 = lighter than baseline → anomalously quiet.\n"
        "• if_flagged M/N: IsolationForest detected a TRAFFIC ANOMALY in M of N windows. NOT a data-quality flag.\n"
        "• both_flagged M/N: BOTH signals confirmed — highest confidence anomaly.\n"
        "• Write 'IF flagged' or 'IF not flagged' only — never assign IF a number."
    ),
}


_ZONE_LANDMARK: dict[str, str] = {
    "1": "Chợ Bến Thành (P. Bến Thành, TP.HCM)",
    "2": "Khu CNC Sài Gòn - SHTP (P. Tăng Nhơn Phú, TP.HCM)",
    "3": "KCN Mỹ Phước (P. Thới Hòa, TP.HCM)",
    "4": "Cảng Cát Lái (P. Cát Lái, TP.HCM)",
    "5": "KCN Lê Minh Xuân (P. Lê Minh Xuân, TP.HCM)",
    "6": "Cảng Phú Mỹ (P. Phú Mỹ, TP.HCM)",
}


def _zone_route_label(route_id: str) -> str:
    m = re.match(r"^zone(\d+)_.*_to_zone(\d+)", route_id)
    if m:
        src = _ZONE_LANDMARK.get(m.group(1), f"Zone {m.group(1)}")
        dst = _ZONE_LANDMARK.get(m.group(2), f"Zone {m.group(2)}")
        return f"{src} → {dst}"
    return route_id.replace("_to_", " → ").replace("_", " ").title()


def _build_heatmap_figure(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Build priority_bar figure from heatmap rows — top anomalous routes by peak z-score."""
    by_route: dict[str, dict[str, Any]] = {}
    for r in rows:
        rid = str(r.get("route_id") or "")
        z = float(r.get("duration_zscore") or 0.0)
        is_a = bool(r.get("is_anomaly"))
        if_a = bool(r.get("iforest_anomaly"))
        both = bool(r.get("both_anomaly"))
        if not (is_a or if_a or both):
            continue
        cur = by_route.get(rid)
        if cur is None or z > cur["peak_z"]:
            by_route[rid] = {
                "peak_z": z,
                "signal": "both" if both else "zscore" if is_a else "iforest",
                "anomaly": both or is_a,
            }

    if not by_route:
        return None

    bars = sorted(by_route.items(), key=lambda x: x[1]["peak_z"], reverse=True)[:8]
    return {
        "type": "priority_bar",
        "title_vi": "Tuyến bất thường — Z-score đỉnh",
        "title_en": "Anomalous Routes — Peak Z-Score",
        "snapshot_time": "",
        "bars": [
            {
                "route": _zone_route_label(rid),
                "peak_z": round(info["peak_z"], 2),
                "signal": info["signal"],
                "anomaly": info["anomaly"],
            }
            for rid, info in bars
        ],
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
        # expected max n per (dow, hour) slot = floor(span_days / 7)
        span_days = (span_h // 24) if span_h else 7
        max_n = max(1, span_days // 7)
        if max_n >= 3:
            recur_note_vi = "Ưu tiên MẪU LẶP LẠI xuất hiện ≥3 lần (ví dụ: 'mỗi thứ 6 lúc 17h'). Bỏ qua sự kiện đơn lẻ không lặp lại."
            recur_note_en = "Prioritise RECURRING PATTERNS appearing ≥3 times (e.g. 'every Friday at 17:00'). Ignore one-off incidents."
        elif max_n == 2:
            recur_note_vi = f"Window {span_days} ngày: n tối đa ~{max_n} — báo cáo slot xuất hiện ≥2 lần; nếu chỉ n=1 thì ghi nhận là 'quan sát đơn lẻ, cần theo dõi thêm'."
            recur_note_en = f"{span_days}-day window: max n~{max_n} — report slots appearing ≥2 times; if only n=1, note as 'single observation, needs monitoring'."
        else:
            recur_note_vi = f"Window {span_days} ngày: n=1 là BÌNH THƯỜNG — báo cáo các slot có tín hiệu mạnh nhất (both_flagged hoặc z_avg cao nhất). Không yêu cầu lặp lại."
            recur_note_en = f"{span_days}-day window: n=1 is NORMAL — report slots with strongest signals (both_flagged or highest z_avg). Recurrence not required."
        if lang == "vi":
            concise = f"3–4 câu mỗi mục. Không chào hỏi. Mục 4 phải gắn khuyến nghị với ngày trong tuần + khung giờ (UTC+7). {recur_note_vi}"
        else:
            concise = f"3–4 sentences per section. No greetings. Section 4 must anchor recommendations to day-of-week + hour range (UTC+7). {recur_note_en}"
    else:
        sections = "\n".join(_SECTIONS.get(lang, _SECTIONS["en"]))
        concise = (
            "3–4 câu mỗi mục. Không chào hỏi. Mục 4 phải có giờ cụ thể (UTC+7) cho mỗi khuyến nghị."
            if lang == "vi"
            else "3–4 sentences per section. No greetings. Section 4 must include an explicit hour range (UTC+7) for every recommendation."
        )

    lang_note = _LANG_INSTRUCTIONS.get(lang, _LANG_INSTRUCTIONS["en"])
    time_header = _format_window_header(lang, window_from, window_to)
    checklist = _SIGNAL_CHECKLIST.get(lang, _SIGNAL_CHECKLIST["en"])
    if multi_day:
        n_note = (
            "• n = số lần slot (ngày_trong_tuần, giờ) xuất hiện trong window. "
            f"Window {(span_h or 168) // 24} ngày → n tối đa ~{max(1, (span_h or 168) // 168)}. n=1 là bình thường, KHÔNG phải lỗi dữ liệu."
            if lang == "vi"
            else "• n = occurrences of (day-of-week, hour) slot in the window. "
            f"{(span_h or 168) // 24}-day window → max n~{max(1, (span_h or 168) // 168)}. n=1 is normal, NOT a data-quality issue."
        )
        heatmap_block = f"{checklist}\n{n_note}\n\n=== HEATMAP DATA ===\n{context}"
    else:
        heatmap_block = f"{checklist}\n\n=== HEATMAP DATA ===\n{context}"
    parts = [
        time_header,
        f"Provide analysis in 4 sections:\n{sections}\n{concise}",
        heatmap_block,
    ]
    if external:
        parts.append(external)
    trigger = "Bắt đầu phân tích:" if lang == "vi" else "Begin analysis:"
    parts.append(f"{lang_note}\n\n{trigger}")
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
        # window_start from Postgres is already UTC+7 — use directly
        key = (r["route_id"], ws.weekday(), ws.hour)
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

    # Fetch heatmap rows for figure (both single-day and multiday)
    rows: list[dict[str, Any]] = []
    if req.window_from and req.window_to:
        try:
            frm_dt = datetime.fromisoformat(req.window_from)
            to_dt = datetime.fromisoformat(req.window_to)
            rows = await metrics_repo.fetch_heatmap_range(conn, frm_dt, to_dt)
        except Exception as exc:
            logger.warning("heatmap fetch for figure failed: %s", exc)

    figure = _build_heatmap_figure(rows)
    figure_event = f"data: {json.dumps({'figure': figure})}\n\n" if figure else ""

    if span_h is not None and span_h > 24:
        # Multiday: Python pre-processor → 2-step LLM chain
        span_days = span_h // 24
        summary = preprocess(rows, span_days)
        rag_context = await fetch_rag_for_multiday(summary.get("top5_anomalies", []))

        async def _multiday_stream() -> Any:
            async for chunk in run_multiday_chain(summary, rag_context, req.lang, req.window_from, req.window_to):
                # Intercept done: yield figure first so frontend receives it before closing
                if figure_event and '"done"' in chunk:
                    try:
                        if json.loads(chunk[len("data:"):].strip()).get("done"):
                            yield figure_event
                            yield chunk
                            return
                    except Exception:
                        pass
                yield chunk
            if figure_event:
                yield figure_event

        return StreamingResponse(
            _multiday_stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    weather = await fetch_current_weather()
    external = await fetch_heatmap_external_context(req.route_ids, weather)
    lang_note = _LANG_INSTRUCTIONS.get(req.lang, _LANG_INSTRUCTIONS["en"])
    system = f"{_ANALYZE_SYSTEM_BASE} {lang_note}"
    user_prompt = _build_analyze_prompt(
        context, req.lang, external, req.window_from, req.window_to
    )

    async def _singleday_stream() -> Any:
        async for chunk in stream_ollama(system, user_prompt):
            if figure_event and '"done"' in chunk:
                try:
                    if json.loads(chunk[len("data:"):].strip()).get("done"):
                        yield figure_event
                        yield chunk
                        return
                except Exception:
                    pass
            yield chunk
        if figure_event:
            yield figure_event

    return StreamingResponse(
        _singleday_stream(),
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
