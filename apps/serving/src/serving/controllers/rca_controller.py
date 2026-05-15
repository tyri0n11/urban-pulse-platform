"""RCA controller — RAG retrieval and prompt assembly for root cause analysis."""

import re
from datetime import datetime, timezone
from typing import Any


def _short_name(route_id: str) -> str:
    parts = route_id.split("_to_")
    if len(parts) != 2:
        return route_id

    def clean(s: str) -> str:
        return re.sub(r"^zone\d+_", "", s).replace("_", " ").title()

    return f"{clean(parts[0])} → {clean(parts[1])}"


async def retrieve_context(
    message: str, route_id: str | None
) -> tuple[list[Any], str]:
    """Retrieve RAG context; returns (chunks, formatted_rag_context)."""
    from rag.client import get_chroma_client
    from rag.retriever import retrieve_for_route, retrieve_for_query, format_chunks_for_prompt
    import logging

    logger = logging.getLogger(__name__)
    chunks: list[Any] = []

    if route_id:
        now = datetime.now(timezone.utc)
        hour = now.hour
        dow = (now.weekday() + 1) % 7
        try:
            chroma = get_chroma_client()
            chunks = retrieve_for_route(
                chroma, route_id, hour=hour, dow=dow,
                n_anomaly=4, n_pattern=3, n_external=2,
            )
        except Exception as exc:
            logger.debug("rca: route retrieval failed — %s", exc)

    if not chunks:
        try:
            chroma = get_chroma_client()
            chunks = retrieve_for_query(chroma, message, n_results=5)
        except Exception as exc:
            logger.debug("rca: free-text retrieval failed — %s", exc)

    return chunks, format_chunks_for_prompt(chunks)


def _build_live_snapshot_section(
    row_list: list[dict[str, Any]],
    iforest_by_route: dict[str, bool],
) -> str:
    """Format current anomaly snapshot for injection into RCA prompt."""
    if not row_list:
        return ""

    anomalies = []
    for row in row_list:
        is_z = row.get("is_anomaly", False)
        is_if = iforest_by_route.get(row["route_id"], False)
        if is_z or is_if:
            if is_z and is_if:
                sig = "BOTH"
            elif is_z:
                sig = "ZSCORE"
            else:
                sig = "IFOREST"
            anomalies.append({
                "route_id": row["route_id"],
                "route": _short_name(row["route_id"]),
                "signal": sig,
                "heavy_pct": round((row.get("mean_heavy_ratio") or 0) * 100, 1),
                "moderate_pct": round((row.get("mean_moderate_ratio") or 0) * 100, 1),
                "severe_seg": int(row.get("max_severe_segments") or 0),
            })

    anomalies.sort(key=lambda x: x["heavy_pct"], reverse=True)

    lines = [
        "=== LIVE SYSTEM SNAPSHOT (current anomalies only — cite ONLY these) ===",
        f"Total monitored routes: {len(row_list)}",
        f"Routes with anomalies now: {len(anomalies)}",
        "IMPORTANT: These are the ONLY routes currently flagged. Do NOT mention routes not listed here.",
        "",
    ]

    if anomalies:
        lines.append("Anomalous routes (sorted by heavy_ratio):")
        for a in anomalies:
            lines.append(
                f"  [{a['signal']}] {a['route']} (id={a['route_id']}): "
                f"heavy={a['heavy_pct']:.1f}%, moderate={a['moderate_pct']:.1f}%, "
                f"severe_segments={a['severe_seg']}"
            )
    else:
        lines.append("No anomalies detected at this moment.")

    lines.append("=== END LIVE SNAPSHOT ===")
    return "\n".join(lines)


def build_rca_prompt(
    rag_context: str,
    message: str,
    lang: str,
    row_list: list[dict[str, Any]] | None = None,
    iforest_by_route: dict[str, bool] | None = None,
) -> str:
    lang_note = "Trả lời bằng tiếng Việt." if lang == "vi" else "Respond in English."

    parts: list[str] = []

    snapshot_section = _build_live_snapshot_section(
        row_list or [], iforest_by_route or {}
    )
    if snapshot_section:
        parts.append(snapshot_section)

    if rag_context:
        parts.append(rag_context)

    parts.append(lang_note)
    parts.append(f"Câu hỏi phân tích: {message}")

    return "\n\n".join(parts)


def chunks_to_log_data(chunks: list[Any]) -> list[dict[str, Any]]:
    return [{"text": c.text, "score": c.score, "collection": c.collection} for c in chunks]
