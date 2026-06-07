"""Multiday traffic analysis pipeline.

Python pre-processor → LLM classify (JSON) → LLM stream narrative.
LLM never sees raw rows — only pre-computed compact summary JSON.
"""

import json
import logging
import re
from collections import defaultdict
from datetime import date, datetime
from typing import Any, AsyncGenerator
from zoneinfo import ZoneInfo

import numpy as np

from serving.services.llm_service import ask_llm, stream_ollama

logger = logging.getLogger(__name__)
_HCMC_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

_ZONE_LANDMARK: dict[str, str] = {
    "1": "Chợ Bến Thành (P. Bến Thành, TP.HCM)",
    "2": "Khu CNC Sài Gòn - SHTP (P. Tăng Nhơn Phú, TP.HCM)",
    "3": "KCN Mỹ Phước (P. Thới Hòa, TP.HCM)",
    "4": "Cảng Cát Lái (P. Cát Lái, TP.HCM)",
    "5": "KCN Lê Minh Xuân (P. Lê Minh Xuân, TP.HCM)",
    "6": "Cảng Phú Mỹ (P. Phú Mỹ, TP.HCM)",
}


def _route_label(route_id: str) -> str:
    """zone1_urban_core_to_zone4_southern_port → 'Chợ Bến Thành ... → Cảng Cát Lái ...'"""
    m = re.match(r"^zone(\d+)_.*_to_zone(\d+)", route_id)
    if m:
        src = _ZONE_LANDMARK.get(m.group(1), f"Zone {m.group(1)}")
        dst = _ZONE_LANDMARK.get(m.group(2), f"Zone {m.group(2)}")
        return f"{src} → {dst}"
    return route_id.replace("_to_", " → ").replace("_", " ").title()


_BUCKETS: list[tuple[str, set[int]]] = [
    ("morning_06_09", {6, 7, 8, 9}),
    ("midday_10_13", {10, 11, 12, 13}),
    ("afternoon_14_17", {14, 15, 16, 17}),
    ("evening_18_21", {18, 19, 20, 21}),
    ("night_22_05", {22, 23, 0, 1, 2, 3, 4, 5}),
]


def _bucket(hour: int) -> str:
    for name, hours in _BUCKETS:
        if hour in hours:
            return name
    return "night_22_05"


def _ws_dt(r: dict[str, Any]) -> datetime | None:
    """Return window_start as datetime. Postgres returns it already in UTC+7 — do NOT re-convert."""
    ws = r.get("window_start")
    if ws is None:
        return None
    return datetime.fromisoformat(ws) if isinstance(ws, str) else ws  # type: ignore[return-value]


# ── Pre-processor ─────────────────────────────────────────────────────────────

def preprocess(rows: list[dict[str, Any]], span_days: int) -> dict[str, Any]:
    """Compute compact analytics summary from raw heatmap rows.

    Filters: is_anomaly=True AND z > 0 (negative z-scores dropped per spec).
    Returns small JSON-serialisable dict — LLM never sees raw rows.
    """
    anomaly_rows = [
        r for r in rows
        if r.get("is_anomaly") and (r.get("duration_zscore") or 0) > 0
    ]
    if not anomaly_rows:
        return {"empty": True, "span_days": span_days}

    by_route: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in anomaly_rows:
        by_route[r["route_id"]].append(r)

    route_stats: list[dict[str, Any]] = []
    for route_id, route_rows in by_route.items():
        zscores = [float(r["duration_zscore"]) for r in route_rows if r.get("duration_zscore")]
        if not zscores:
            continue
        peak_z = max(zscores)

        days: set[date] = set()
        hour_zs: dict[int, list[float]] = defaultdict(list)
        for r in route_rows:
            dt = _ws_dt(r)
            if dt:
                # window_start from Postgres is already UTC+7 — use directly
                days.add(dt.date())
                if r.get("duration_zscore"):
                    hour_zs[dt.hour].append(float(r["duration_zscore"]))

        peak_hour = int(max(hour_zs, key=lambda h: float(np.mean(hour_zs[h])))) if hour_zs else 0

        # Trend slope via numpy polyfit (no scipy required)
        sorted_rows = sorted(route_rows, key=lambda r: str(r.get("window_start") or ""))
        zs_arr = np.array([float(r["duration_zscore"]) for r in sorted_rows if r.get("duration_zscore")])
        slope = 0.0
        if len(zs_arr) >= 3:
            slope = float(np.polyfit(np.arange(len(zs_arr)), zs_arr, 1)[0])
        trend = "worsening" if slope > 0.01 else "improving" if slope < -0.01 else "stable"

        severity = "critical" if peak_z > 4 else "warning" if peak_z > 2.5 else "info"
        both = any(r.get("both_anomaly") for r in route_rows)
        label = _route_label(route_id)

        route_stats.append({
            "route_id": route_id,
            "route": label,
            "peak_z": round(peak_z, 2),
            "severity": severity,
            "peak_hour_utc7": peak_hour,
            "peak_bucket": _bucket(peak_hour),
            "flagged_days": len(days),
            "recurring": len(days) >= 2,
            "trend": trend,
            "both_confirmed": bool(both),
        })

    route_stats.sort(key=lambda x: x["peak_z"], reverse=True)
    top5 = route_stats[:5]

    # Peak hour clusters
    bucket_routes: dict[str, list[str]] = defaultdict(list)
    for s in route_stats:
        bucket_routes[s["peak_bucket"]].append(s["route"])

    # Route correlation (pearson, routes with ≥3 anomaly windows)
    correlated: list[dict[str, Any]] = []
    all_windows = sorted({dt for r in anomaly_rows if (dt := _ws_dt(r)) is not None})
    if len(all_windows) > 1:
        win_idx = {w: i for i, w in enumerate(all_windows)}
        ts_map: dict[str, np.ndarray] = {}
        for route_id, route_rows in by_route.items():
            arr = np.zeros(len(all_windows))
            for r in route_rows:
                dt = _ws_dt(r)
                if dt and r.get("duration_zscore"):
                    idx = win_idx.get(dt)
                    if idx is not None:
                        arr[idx] = float(r["duration_zscore"])
            if int(np.count_nonzero(arr)) >= 3:
                ts_map[route_id] = arr

        ids = list(ts_map.keys())
        if len(ids) >= 2:
            corr = np.corrcoef(np.stack(list(ts_map.values())))
            for i in range(len(ids)):
                for j in range(i + 1, len(ids)):
                    val = float(corr[i, j])
                    if not np.isnan(val) and val > 0.7:
                        correlated.append({
                            "pair": [
                                _route_label(ids[i]),
                                _route_label(ids[j]),
                            ],
                            "r": round(val, 2),
                        })
        correlated.sort(key=lambda x: x["r"], reverse=True)

    trend_groups: dict[str, list[str]] = defaultdict(list)
    for s in route_stats:
        trend_groups[s["trend"]].append(s["route"])

    return {
        "span_days": span_days,
        "total_anomaly_windows": len(anomaly_rows),
        "affected_routes": len(by_route),
        "top5_anomalies": top5,
        "peak_clusters": {k: v for k, v in bucket_routes.items() if v},
        "correlated_routes": correlated[:5],
        "trend_groups": {k: v for k, v in trend_groups.items() if v},
    }


# ── RAG retrieval ─────────────────────────────────────────────────────────────

async def fetch_rag_for_multiday(top5: list[dict[str, Any]]) -> str:
    """Retrieve anomaly + weather docs for the top critical routes."""
    if not top5:
        return ""
    try:
        from rag.client import get_chroma_client
        from rag.retriever import retrieve_for_route, format_chunks_for_prompt

        chroma = get_chroma_client()
        all_chunks = []
        for s in top5[:3]:
            chunks = retrieve_for_route(
                chroma,
                s["route_id"],
                hour=s["peak_hour_utc7"],
                dow=0,
                n_anomaly=2,
                n_pattern=1,
                n_external=1,
            )
            all_chunks.extend(chunks)

        if not all_chunks:
            return ""

        seen: set[str] = set()
        unique = []
        for c in sorted(all_chunks, key=lambda x: x.score):
            if c.text not in seen:
                seen.add(c.text)
                unique.append(c)

        return (
            "=== RAG CONTEXT (historical anomalies + patterns) ===\n"
            + format_chunks_for_prompt(unique[:6])
        )
    except Exception as exc:
        logger.debug("multiday RAG failed: %s", exc)
        return ""


# ── LLM prompts ───────────────────────────────────────────────────────────────

_CLASSIFY_SYSTEM = (
    "You are a traffic anomaly classifier. "
    "INPUT: JSON summary of detected traffic anomalies for Ho Chi Minh City. "
    "OUTPUT: JSON only. No prose. No markdown. "
    "Rules: "
    "(1) Classify each of the top5_anomalies by type: PEAK_HOUR | INCIDENT | SPECIAL_EVENT | INFRASTRUCTURE | UNKNOWN. "
    "(2) Use ONLY route names from input — never invent routes. "
    "(3) evidence must cite specific values: peak_z, peak_hour_utc7, recurring, both_confirmed. "
    "(4) If correlated route pairs share a common cause, set shared_cause to the other route name. "
    "Output schema exactly: "
    '{"classified":[{"route":"...","severity":"critical|warning|info","type":"...","evidence":"...","shared_cause":"...or null"}],'
    '"systemic_patterns":["...one sentence per pattern across multiple routes"]}'
)

_NARRATIVE_SYSTEM_VI = (
    "Bạn là chuyên gia phân tích giao thông đô thị TP.HCM, viết báo cáo cho ban quản lý vận hành đô thị. "
    "Quy tắc bắt buộc: "
    "(1) Chỉ dùng dữ liệu từ input — không bịa tuyến, giờ, hay con số. "
    "(2) TUYỆT ĐỐI KHÔNG dùng tên biến kỹ thuật trong văn bản: không viết 'peak_z', 'both_confirmed', "
    "'severity', 'recurring', 'peak_hour_utc7', 'iforest', 'zscore', 'flagged'. "
    "Thay vào đó dùng ngôn ngữ tự nhiên: "
    "'severity=critical' → 'nghiêm trọng'; "
    "'both_confirmed=true' → 'được xác nhận bởi cả hai hệ thống giám sát'; "
    "'recurring=true' → 'xuất hiện lặp lại nhiều ngày'; "
    "'peak_hour_utc7=3' → 'lúc 03:00'; "
    "'trend=worsening' → 'đang có xu hướng xấu dần'. "
    "(3) Viết đúng 3–4 câu mỗi mục. Không chào hỏi. "
    "QUAN TRỌNG: Toàn bộ phân tích phải viết bằng tiếng Việt."
)

_NARRATIVE_SYSTEM_EN = (
    "You are an urban traffic analyst for Ho Chi Minh City, writing reports for urban operations managers. "
    "Mandatory rules: "
    "(1) Use ONLY data from input — never invent routes, hours, or numbers. "
    "(2) NEVER use technical variable names in the output: do not write 'peak_z', 'both_confirmed', "
    "'severity', 'recurring', 'peak_hour_utc7', 'iforest', 'zscore', 'flagged'. "
    "Translate to natural language: "
    "'severity=critical' → 'severe'; "
    "'both_confirmed=true' → 'confirmed by both monitoring systems'; "
    "'recurring=true' → 'recurring across multiple days'; "
    "'peak_hour_utc7=3' → 'at 03:00'; "
    "'trend=worsening' → 'showing a worsening trend'. "
    "(3) Write exactly 3–4 sentences per section. No greetings. "
    "IMPORTANT: Write the entire analysis in English."
)

_SECTIONS_VI = (
    "Viết báo cáo theo 4 mục:\n"
    "1. **Mẫu bất thường nổi bật** — liệt kê top routes theo peak_z và both_confirmed. "
    "Nêu rõ peak_hour_utc7 và severity. Chú thích recurring nếu xuất hiện ≥2 ngày.\n"
    "2. **Nguyên nhân gốc rễ** — liên kết type phân loại (PEAK_HOUR/INCIDENT...) với đặc điểm "
    "giao thông TP.HCM (peak sáng/chiều, KCN, cảng, cuối tuần). Đề cập correlated_routes nếu r>0.7.\n"
    "3. **Xu hướng và tương quan** — mô tả trend_groups (worsening/improving/stable) "
    "và cặp route có r cao. Chỉ nêu xu hướng, không lặp lại số liệu thô.\n"
    "4. **Khuyến nghị** — mỗi khuyến nghị PHẢI gắn route cụ thể + khung giờ UTC+7. "
    "Tập trung vào các route severity=critical hoặc both_confirmed=true."
)

_SECTIONS_EN = (
    "Write the report in 4 sections:\n"
    "1. **Top anomaly patterns** — list top routes by peak_z and both_confirmed. "
    "State peak_hour_utc7 and severity. Note recurring if flagged_days ≥ 2.\n"
    "2. **Root causes** — link classification type (PEAK_HOUR/INCIDENT...) to HCMC traffic context "
    "(morning/evening peak, industrial zones, port logistics, weekday vs weekend). Mention correlated route pairs if r>0.7.\n"
    "3. **Trends and correlations** — describe trend_groups (worsening/improving/stable) "
    "and high-correlation route pairs. Do not repeat raw numbers.\n"
    "4. **Recommendations** — each recommendation MUST specify a route and UTC+7 hour range. "
    "Prioritise routes with severity=critical or both_confirmed=true."
)


def _fmt_date_utc7(iso: str | None, lang: str) -> str:
    """Parse UTC ISO string → date only in UTC+7, formatted for display."""
    if not iso:
        return "?"
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        dt_vn = dt.astimezone(_HCMC_TZ)
        return dt_vn.strftime("%d/%m/%Y") if lang == "vi" else dt_vn.strftime("%b %d, %Y")
    except Exception:
        return iso


def _build_narrative_user(
    classify_result: str,
    summary: dict[str, Any],
    rag_context: str,
    lang: str,
    window_from: str | None,
    window_to: str | None,
) -> str:
    span_days = summary.get("span_days", 7)
    sections = _SECTIONS_VI if lang == "vi" else _SECTIONS_EN
    trigger = "Bắt đầu phân tích:" if lang == "vi" else "Begin analysis:"

    date_from = _fmt_date_utc7(window_from, lang)
    date_to = _fmt_date_utc7(window_to, lang)
    if lang == "vi":
        window_label = f"Phân tích từ ngày {date_from} đến ngày {date_to} (UTC+7) · {span_days} ngày"
    else:
        window_label = f"Analysis window: {date_from} → {date_to} (UTC+7) · {span_days} days"

    payload = {
        "window_utc7": f"{date_from} → {date_to}",
        "span_days": span_days,
        "top5_anomalies": summary.get("top5_anomalies", []),
        "peak_clusters": summary.get("peak_clusters", {}),
        "correlated_routes": summary.get("correlated_routes", []),
        "trend_groups": summary.get("trend_groups", {}),
        "classification": classify_result,
    }

    parts = [
        window_label,
        sections,
        f"=== PRE-COMPUTED ANALYTICS ===\n{json.dumps(payload, ensure_ascii=False, indent=2)}",
    ]
    if rag_context:
        parts.append(rag_context)
    parts.append(trigger)
    return "\n\n".join(parts)


# ── Chain orchestrator ────────────────────────────────────────────────────────

async def run_multiday_chain(
    summary: dict[str, Any],
    rag_context: str,
    lang: str,
    window_from: str | None,
    window_to: str | None,
) -> AsyncGenerator[str, None]:
    """Two-step LLM chain: classify (JSON, non-streaming) → narrative (streaming)."""
    if summary.get("empty"):
        msg = (
            "Không có bất thường nào với z-score > 0 trong khoảng thời gian đã chọn."
            if lang == "vi"
            else "No positive-Z anomalies detected in the selected window."
        )
        yield f"data: {json.dumps({'chunk': msg})}\n\n"
        yield f"data: {json.dumps({'done': True})}\n\n"
        return

    # Step 1 — classify (non-streaming, JSON)
    step1_label = "Đang phân loại bất thường..." if lang == "vi" else "Classifying anomalies..."
    yield f"data: {json.dumps({'status': step1_label})}\n\n"

    classify_input = json.dumps(
        {
            "top5_anomalies": summary.get("top5_anomalies", []),
            "peak_clusters": summary.get("peak_clusters", {}),
            "correlated_routes": summary.get("correlated_routes", []),
        },
        ensure_ascii=False,
    )
    classify_result = ""
    try:
        raw = await ask_llm(
            _CLASSIFY_SYSTEM,
            f"Classify these anomalies:\n{classify_input}",
            temperature=0.05,
            json_mode=True,
        )
        parsed = json.loads(raw)
        classify_result = json.dumps(parsed, ensure_ascii=False)
    except Exception as exc:
        logger.warning("multiday classify failed: %s", exc)
        classify_result = json.dumps({"classified": summary.get("top5_anomalies", [])}, ensure_ascii=False)

    # Step 2 — narrative (streaming)
    step2_label = "Đang viết báo cáo..." if lang == "vi" else "Generating report..."
    yield f"data: {json.dumps({'status': step2_label})}\n\n"

    system = _NARRATIVE_SYSTEM_VI if lang == "vi" else _NARRATIVE_SYSTEM_EN
    user_prompt = _build_narrative_user(classify_result, summary, rag_context, lang, window_from, window_to)

    async for chunk in stream_ollama(system, user_prompt):
        yield chunk
