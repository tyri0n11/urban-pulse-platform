"""Anomaly alerter — checks for active anomalies and sends Telegram notifications.

Runs every 5 minutes via Prefect. Deduplicates via anomaly_alert_log table:
a route is only re-alerted after ALERT_COOLDOWN_MINUTES of silence.

Alert priority: BOTH (z-score + IForest) > IFOREST only > ZSCORE only.
Only IFOREST or BOTH signals are alerted — z-score-only is too noisy.
"""

import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
import psycopg2

logger = logging.getLogger(__name__)

_PG_DSN = os.getenv(
    "DATABASE_URL",
    "postgresql://urbanpulse:urbanpulse@postgres:5432/urbanpulse",
)
_TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
_TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
_OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434")
_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:3b")
_ALERT_COOLDOWN_MINUTES = 30


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _ensure_alert_table(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS anomaly_alert_log (
                id          SERIAL PRIMARY KEY,
                alerted_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                route_id    TEXT        NOT NULL,
                signal      TEXT        NOT NULL,
                message     TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_alert_log_route_ts
                ON anomaly_alert_log (route_id, alerted_at DESC);
        """)
        conn.commit()


def _fetch_active_anomalies(conn: Any) -> list[dict[str, Any]]:
    """Return routes with IForest or BOTH anomaly signal in the last 10 min."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT ON (o.route_id)
                o.route_id,
                o.window_start,
                o.mean_heavy_ratio,
                COALESCE(o.mean_moderate_ratio, 0.0) AS mean_moderate_ratio,
                COALESCE(o.max_severe_segments, 0)   AS max_severe_segments,
                o.is_anomaly                          AS zscore_anomaly,
                COALESCE(
                    CASE WHEN i.score_count > 0
                         THEN i.anomaly_count::float / i.score_count >= 0.5
                         ELSE i.iforest_anomaly
                    END,
                    false
                ) AS iforest_anomaly,
                COALESCE(
                    CASE WHEN i.score_count > 0
                         THEN i.both_count::float / i.score_count >= 0.5
                         ELSE i.both_anomaly
                    END,
                    false
                ) AS both_anomaly
            FROM online_route_features o
            LEFT JOIN route_iforest_scores i
                ON o.route_id = i.route_id
               AND o.window_start = i.window_start
            WHERE o.updated_at >= NOW() - INTERVAL '10 minutes'
            ORDER BY o.route_id, o.updated_at DESC
        """)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]

    # Only alert on IForest-confirmed signals (more reliable than z-score alone)
    return [r for r in rows if r["iforest_anomaly"] or r["both_anomaly"]]


def _recently_alerted(conn: Any, route_id: str) -> bool:
    cooldown = datetime.now(timezone.utc) - timedelta(minutes=_ALERT_COOLDOWN_MINUTES)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM anomaly_alert_log WHERE route_id = %s AND alerted_at >= %s LIMIT 1",
            (route_id, cooldown),
        )
        return cur.fetchone() is not None


def _log_alert(conn: Any, route_id: str, signal: str, message: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO anomaly_alert_log (route_id, signal, message) VALUES (%s, %s, %s)",
            (route_id, signal, message),
        )
        conn.commit()


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------

def _short_name(route_id: str) -> str:
    parts = route_id.split("_to_")
    if len(parts) != 2:
        return route_id
    clean = lambda s: re.sub(r"^zone\d+_", "", s).replace("_", " ").title()
    return f"{clean(parts[0])} → {clean(parts[1])}"


# ---------------------------------------------------------------------------
# LLM analysis
# ---------------------------------------------------------------------------

def _generate_analysis(row: dict[str, Any]) -> str:
    """Call Ollama for a 1-sentence Vietnamese root-cause hint."""
    if not _OLLAMA_URL:
        return ""

    now = datetime.now(timezone.utc)
    hour = now.hour
    day_type = "cuối tuần" if now.weekday() >= 5 else "ngày thường"

    prompt = (
        f"Tuyến: {_short_name(row['route_id'])}\n"
        f"Thời điểm: {hour}h UTC ({day_type})\n"
        f"heavy_ratio={round((row['mean_heavy_ratio'] or 0) * 100, 1)}%, "
        f"moderate_ratio={round((row['mean_moderate_ratio'] or 0) * 100, 1)}%, "
        f"severe_segments={row['max_severe_segments']}\n"
        f"Tín hiệu: {'BOTH (z-score + IForest)' if row.get('both_anomaly') else 'IForest'}\n"
        "Trong 1 câu tiếng Việt ngắn gọn: nguyên nhân khả năng nhất là gì?"
    )

    system = (
        "Bạn là trợ lý phân tích giao thông TPHCM. "
        "Trả lời đúng 1 câu tiếng Việt súc tích. Không chào hỏi."
    )

    try:
        resp = httpx.post(
            f"{_OLLAMA_URL}/api/generate",
            json={
                "model": _MODEL,
                "system": system,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.3, "num_predict": 80},
            },
            timeout=30.0,
        )
        resp.raise_for_status()
        return resp.json().get("response", "").strip()
    except Exception as exc:
        logger.warning("alerter: LLM call failed — %s", exc)
        return ""


# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------

def _send_telegram(text: str) -> bool:
    if not _TELEGRAM_BOT_TOKEN or not _TELEGRAM_CHAT_ID:
        logger.warning("alerter: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not configured")
        return False
    try:
        resp = httpx.post(
            f"https://api.telegram.org/bot{_TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": _TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=10.0,
        )
        resp.raise_for_status()
        return True
    except Exception as exc:
        logger.error("alerter: Telegram send failed — %s", exc)
        return False


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run() -> int:
    """Check for new anomalies and send alerts. Returns number of alerts sent."""
    conn = psycopg2.connect(_PG_DSN)
    try:
        _ensure_alert_table(conn)
        anomalies = _fetch_active_anomalies(conn)

        if not anomalies:
            logger.info("alerter: no active anomalies")
            return 0

        logger.info("alerter: %d active anomaly routes found", len(anomalies))
        sent = 0

        for row in anomalies:
            route_id = row["route_id"]

            if _recently_alerted(conn, route_id):
                logger.debug("alerter: skipping %s (cooldown active)", route_id)
                continue

            signal = "BOTH" if row["both_anomaly"] else "IFOREST"
            heavy = round((row["mean_heavy_ratio"] or 0) * 100, 1)
            moderate = round((row["mean_moderate_ratio"] or 0) * 100, 1)
            severe = int(row["max_severe_segments"] or 0)

            analysis = _generate_analysis(row)

            now_str = datetime.now(timezone.utc).strftime("%H:%M UTC")
            msg = (
                f"🚨 <b>Urban Pulse Alert</b> [{now_str}]\n"
                f"📍 <b>{_short_name(route_id)}</b>\n"
                f"Tín hiệu: <b>{signal}</b>\n"
                f"Heavy: {heavy}% | Moderate: {moderate}% | Severe segs: {severe}"
            )
            if analysis:
                msg += f"\n\n💡 {analysis}"

            if _send_telegram(msg):
                _log_alert(conn, route_id, signal, msg)
                logger.info("alerter: sent alert for %s [%s]", route_id, signal)
                sent += 1

        return sent
    finally:
        conn.close()
