"""Online feature consumer: streams traffic-route-bronze → Postgres.

Uses the same confluent_kafka pattern as streaming-service (proven, no
compatibility issues). Computes per-route rolling stats using Welford's
online algorithm and writes to Postgres on every message, achieving
p95 ingestion-to-feature latency < 20 s.
"""

import json
import logging
import signal
import time
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from urbanpulse_core.config import settings
from urbanpulse_core.models.traffic import TrafficRouteObservation

from online.baseline import load_baseline, BaselineEntry
from online.models import RouteWindow

logger = logging.getLogger(__name__)

TOPIC = "traffic-route-bronze"
GROUP_ID = "online-features-group"

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS online_route_features (
    route_id                TEXT             NOT NULL,
    window_start            TIMESTAMPTZ      NOT NULL,
    updated_at              TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    observation_count       INTEGER          NOT NULL DEFAULT 0,
    mean_duration_minutes   DOUBLE PRECISION,
    stddev_duration_minutes DOUBLE PRECISION,
    last_duration_minutes   DOUBLE PRECISION,
    mean_heavy_ratio        DOUBLE PRECISION,
    last_heavy_ratio        DOUBLE PRECISION,
    duration_zscore         DOUBLE PRECISION,
    is_anomaly              BOOLEAN          NOT NULL DEFAULT FALSE,
    last_ingest_lag_ms      BIGINT,
    heavy_ratio_deviation   DOUBLE PRECISION,
    p95_to_mean_ratio       DOUBLE PRECISION,
    max_severe_segments     DOUBLE PRECISION,
    PRIMARY KEY (route_id, window_start)
);
CREATE INDEX IF NOT EXISTS idx_online_updated_at
    ON online_route_features (updated_at DESC);
-- Migrate existing tables: add columns if they don't exist yet
ALTER TABLE online_route_features ADD COLUMN IF NOT EXISTS heavy_ratio_deviation DOUBLE PRECISION;
ALTER TABLE online_route_features ADD COLUMN IF NOT EXISTS p95_to_mean_ratio     DOUBLE PRECISION;
ALTER TABLE online_route_features ADD COLUMN IF NOT EXISTS max_severe_segments   DOUBLE PRECISION;
"""

_UPSERT_SQL = """
INSERT INTO online_route_features (
    route_id, window_start, updated_at,
    observation_count,
    mean_duration_minutes, stddev_duration_minutes, last_duration_minutes,
    mean_heavy_ratio, last_heavy_ratio,
    duration_zscore, is_anomaly,
    last_ingest_lag_ms,
    heavy_ratio_deviation, p95_to_mean_ratio, max_severe_segments
) VALUES (
    %(route_id)s, %(window_start)s, NOW(),
    %(observation_count)s,
    %(mean_duration_minutes)s, %(stddev_duration_minutes)s, %(last_duration_minutes)s,
    %(mean_heavy_ratio)s, %(last_heavy_ratio)s,
    %(duration_zscore)s, %(is_anomaly)s,
    %(last_ingest_lag_ms)s,
    %(heavy_ratio_deviation)s, %(p95_to_mean_ratio)s, %(max_severe_segments)s
)
ON CONFLICT (route_id, window_start) DO UPDATE SET
    updated_at              = NOW(),
    observation_count       = EXCLUDED.observation_count,
    mean_duration_minutes   = EXCLUDED.mean_duration_minutes,
    stddev_duration_minutes = EXCLUDED.stddev_duration_minutes,
    last_duration_minutes   = EXCLUDED.last_duration_minutes,
    mean_heavy_ratio        = EXCLUDED.mean_heavy_ratio,
    last_heavy_ratio        = EXCLUDED.last_heavy_ratio,
    duration_zscore         = EXCLUDED.duration_zscore,
    is_anomaly              = EXCLUDED.is_anomaly,
    last_ingest_lag_ms      = EXCLUDED.last_ingest_lag_ms,
    heavy_ratio_deviation   = EXCLUDED.heavy_ratio_deviation,
    p95_to_mean_ratio       = EXCLUDED.p95_to_mean_ratio,
    max_severe_segments     = EXCLUDED.max_severe_segments
"""


def _current_hour_ts() -> int:
    now = datetime.now(timezone.utc)
    return int(now.replace(minute=0, second=0, microsecond=0).timestamp())


class OnlineFeatureProcessor:
    """Stateful processor: maintains per-route windows and writes to Postgres."""

    _BASELINE_TTL = 6 * 3600.0

    def __init__(self, pg_dsn: str) -> None:
        self._pg_dsn = pg_dsn
        self._pg = psycopg2.connect(pg_dsn)
        self._pg.autocommit = True
        self._windows: dict[str, RouteWindow] = {}
        self._baseline: dict[str, BaselineEntry] = {}
        self._last_baseline_refresh: float = 0.0

        with self._pg.cursor() as cur:
            cur.execute(_CREATE_TABLE_SQL)
        logger.info("online-features: Postgres ready, table ensured")

        self._refresh_baseline()

    def _reconnect(self) -> None:
        """Re-establish Postgres connection after a drop."""
        try:
            self._pg.close()
        except Exception:
            pass
        self._pg = psycopg2.connect(self._pg_dsn)
        self._pg.autocommit = True
        logger.warning("online-features: reconnected to Postgres")

    def _refresh_baseline(self) -> None:
        try:
            self._baseline = load_baseline()
            self._last_baseline_refresh = time.monotonic()
            logger.info("online-features: baseline loaded — %d entries", len(self._baseline))
        except Exception as exc:
            logger.warning("online-features: baseline unavailable — %s", exc)

    def process(self, msg: Message) -> None:
        # Refresh baseline every 6 h
        if time.monotonic() - self._last_baseline_refresh > self._BASELINE_TTL:
            self._refresh_baseline()

        raw: bytes = msg.value()
        try:
            obs = TrafficRouteObservation.model_validate(json.loads(raw))
        except Exception as exc:
            logger.warning("online-features: parse error — %s", exc)
            return

        # E2E ingestion lag
        lag_ms = 0
        headers = msg.headers() or []
        for key, val in headers:
            if key == "ingest_ts" and val:
                try:
                    lag_ms = int(time.time() * 1000) - int(val.decode())
                except (ValueError, TypeError):
                    pass
                break

        # Update or reset hourly window
        hour_ts = _current_hour_ts()
        window = self._windows.get(obs.route_id)
        if window is None or window.window_start_ts != hour_ts:
            window = RouteWindow(window_start_ts=hour_ts)
            self._windows[obs.route_id] = window

        heavy_ratio = obs.congestion.heavy_ratio if obs.congestion else 0.0
        severe_segments = float(obs.congestion.severe_segments) if obs.congestion else 0.0
        window.update(obs.duration_minutes, heavy_ratio, severe_segments, lag_ms)

        # Z-score vs batch baseline
        baseline = self._baseline.get(obs.route_id)
        zscore: float | None = None
        is_anomaly = False
        heavy_ratio_deviation: float = window.mean_heavy_ratio  # fallback: raw ratio
        if baseline and baseline.stddev > 0:
            zscore = (window.mean_duration - baseline.mean) / baseline.stddev
            is_anomaly = abs(zscore) > 3.0
            heavy_ratio_deviation = window.mean_heavy_ratio - baseline.heavy_ratio_mean

        # p95 approximation: mean + 2*stddev ≈ 97.7th pct under normality
        # Matches training feature (gold.p95_duration / gold.avg_duration)
        p95_approx = window.mean_duration + 2.0 * window.stddev_duration
        p95_to_mean_ratio = (p95_approx / window.mean_duration) if window.mean_duration > 0 else 1.0

        window_start = datetime.fromtimestamp(hour_ts, tz=timezone.utc)
        try:
            with self._pg.cursor() as cur:
                cur.execute(_UPSERT_SQL, {
                    "route_id": obs.route_id,
                    "window_start": window_start,
                    "observation_count": window.count,
                    "mean_duration_minutes": window.mean_duration,
                    "stddev_duration_minutes": window.stddev_duration,
                    "last_duration_minutes": window.last_duration,
                    "mean_heavy_ratio": window.mean_heavy_ratio,
                    "last_heavy_ratio": window.last_heavy_ratio,
                    "duration_zscore": zscore,
                    "is_anomaly": is_anomaly,
                    "last_ingest_lag_ms": lag_ms,
                    "heavy_ratio_deviation": heavy_ratio_deviation,
                    "p95_to_mean_ratio": p95_to_mean_ratio,
                    "max_severe_segments": window.max_severe_segments,
                })
        except psycopg2.OperationalError:
            logger.warning("online-features: Postgres connection lost, reconnecting")
            self._reconnect()
            with self._pg.cursor() as cur:
                cur.execute(_UPSERT_SQL, {
                    "route_id": obs.route_id,
                    "window_start": window_start,
                    "observation_count": window.count,
                    "mean_duration_minutes": window.mean_duration,
                    "stddev_duration_minutes": window.stddev_duration,
                    "last_duration_minutes": window.last_duration,
                    "mean_heavy_ratio": window.mean_heavy_ratio,
                    "last_heavy_ratio": window.last_heavy_ratio,
                    "duration_zscore": zscore,
                    "is_anomaly": is_anomaly,
                    "last_ingest_lag_ms": lag_ms,
                    "heavy_ratio_deviation": heavy_ratio_deviation,
                    "p95_to_mean_ratio": p95_to_mean_ratio,
                    "max_severe_segments": window.max_severe_segments,
                })

        logger.info(
            "online-features: route=%s obs=%d zscore=%s lag_ms=%d",
            obs.route_id, window.count,
            f"{zscore:.2f}" if zscore is not None else "N/A",
            lag_ms,
        )

    def close(self) -> None:
        self._pg.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    running = True

    def _shutdown(sig: int, frame: object) -> None:
        nonlocal running
        logger.info("online-features: shutdown signal received")
        running = False

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    processor = OnlineFeatureProcessor(pg_dsn=settings.postgres_dsn)

    consumer = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",  # replay from last committed offset on restart
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC])
    logger.info("online-features: subscribed to %s", TOPIC)

    try:
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())
            processor.process(msg)
            consumer.commit(asynchronous=False)
    finally:
        consumer.close()
        processor.close()
        logger.info("online-features: stopped cleanly")


if __name__ == "__main__":
    main()
