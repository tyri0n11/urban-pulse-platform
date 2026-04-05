"""Convert PostgreSQL rows and Iceberg records into plain-text RAG documents.

Each formatter returns a (doc_id, text, metadata) tuple.
  doc_id    — stable unique ID so upsert is idempotent
  text      — human-readable text the LLM will receive as context
  metadata  — structured fields for ChromaDB where-filter (route_id, hour, etc.)
"""

from datetime import datetime, timezone
from typing import Any


# ---------------------------------------------------------------------------
# anomaly_events
# ---------------------------------------------------------------------------

def format_anomaly_event(row: dict[str, Any]) -> tuple[str, str, dict[str, Any]]:
    """Format one resolved anomaly window into a retrievable document.

    Expected keys (from online_route_features JOIN route_iforest_scores):
      route_id, window_start, mean_heavy_ratio, mean_moderate_ratio,
      max_severe_segments, duration_zscore, is_anomaly,
      iforest_anomaly, both_anomaly, observation_count
    """
    route_id: str = row["route_id"]
    window_start: datetime = row["window_start"]
    if window_start.tzinfo is None:
        window_start = window_start.replace(tzinfo=timezone.utc)

    heavy = float(row.get("mean_heavy_ratio") or 0)
    moderate = float(row.get("mean_moderate_ratio") or 0)
    severe = int(row.get("max_severe_segments") or 0)
    zscore = row.get("duration_zscore")
    is_z = bool(row.get("is_anomaly", False))
    is_if = bool(row.get("iforest_anomaly", False))
    obs = int(row.get("observation_count") or 0)

    dow_names = ["Chủ nhật", "Thứ Hai", "Thứ Ba", "Thứ Tư", "Thứ Năm", "Thứ Sáu", "Thứ Bảy"]
    dow = dow_names[window_start.weekday() + 1 if window_start.weekday() < 6 else 0]

    signal = "cả Z-Score lẫn IsolationForest" if (is_z and is_if) \
        else "Z-Score" if is_z \
        else "IsolationForest"

    zscore_str = f", duration z-score={zscore:.2f}" if zscore is not None else ""

    text = (
        f"Sự kiện bất thường: {route_id.replace('_', ' ')}\n"
        f"Thời gian: {dow} {window_start.strftime('%Y-%m-%d %H:%M')} UTC\n"
        f"Tín hiệu: {signal}\n"
        f"Heavy ratio: {heavy:.1%}, Moderate ratio: {moderate:.1%}, "
        f"Severe segments: {severe}{zscore_str}\n"
        f"Số quan sát trong cửa sổ: {obs}"
    )

    doc_id = f"anomaly_{route_id}_{window_start.strftime('%Y%m%d%H%M')}"
    metadata = {
        "route_id": route_id,
        "hour": window_start.hour,
        "dow": (window_start.weekday() + 1) % 7,  # SQL DOW: Sun=0
        "heavy_ratio": round(heavy, 4),
        "is_both": is_z and is_if,
        "window_start_ts": int(window_start.timestamp()),
    }
    return doc_id, text, metadata


# ---------------------------------------------------------------------------
# traffic_patterns
# ---------------------------------------------------------------------------

def format_traffic_pattern(row: dict[str, Any]) -> tuple[str, str, dict[str, Any]]:
    """Format one gold.traffic_hourly row as a baseline pattern document.

    Expected keys:
      route_id, hour_utc (or dow + hour_of_day), avg_heavy_ratio,
      avg_moderate_ratio, avg_duration_minutes, observation_count
    """
    route_id: str = row["route_id"]
    dow: int = int(row.get("dow", row.get("day_of_week", 0)))
    hour: int = int(row.get("hour_of_day", row.get("hour", 0)))

    heavy = float(row.get("avg_heavy_ratio") or 0)
    moderate = float(row.get("avg_moderate_ratio") or 0)
    duration = row.get("avg_duration_minutes")
    obs = int(row.get("observation_count") or 0)

    dow_names = ["Chủ nhật", "Thứ Hai", "Thứ Ba", "Thứ Tư", "Thứ Năm", "Thứ Sáu", "Thứ Bảy"]
    dow_name = dow_names[dow] if 0 <= dow <= 6 else str(dow)

    duration_str = f", duration trung bình {duration:.1f} phút" if duration else ""

    text = (
        f"Pattern điển hình: {route_id.replace('_', ' ')}\n"
        f"Thời điểm: {dow_name} {hour:02d}:00 UTC\n"
        f"Heavy ratio TB: {heavy:.1%}, Moderate ratio TB: {moderate:.1%}"
        f"{duration_str}\n"
        f"Dựa trên {obs} giờ lịch sử."
    )

    doc_id = f"pattern_{route_id}_dow{dow}_h{hour:02d}"
    metadata = {
        "route_id": route_id,
        "dow": dow,
        "hour": hour,
        "avg_heavy_ratio": round(heavy, 4),
    }
    return doc_id, text, metadata


# ---------------------------------------------------------------------------
# external_context  (Phase 2 — weather / events / news)
# ---------------------------------------------------------------------------

def format_external_event(row: dict[str, Any]) -> tuple[str, str, dict[str, Any]]:
    """Format one gold.context_hourly row as an external context document."""
    hour_utc: datetime = row["hour_utc"]
    if isinstance(hour_utc, str):
        hour_utc = datetime.fromisoformat(hour_utc)
    if hour_utc.tzinfo is None:
        hour_utc = hour_utc.replace(tzinfo=timezone.utc)

    context_type: str = row.get("context_type", "unknown")
    summary: str = row.get("summary", "")
    source: str = row.get("source", "")

    text = (
        f"External context [{context_type}]: {summary}\n"
        f"Thời gian: {hour_utc.strftime('%Y-%m-%d %H:%M')} UTC\n"
        f"Nguồn: {source}"
    )

    doc_id = f"ext_{context_type}_{hour_utc.strftime('%Y%m%d%H%M')}_{source[:8]}"
    metadata = {
        "context_type": context_type,
        "hour": hour_utc.hour,
        "dow": (hour_utc.weekday() + 1) % 7,
        "hour_ts": int(hour_utc.timestamp()),
        "source": source,
    }
    return doc_id, text, metadata
