"""Fixtures and helpers for integration / smoke tests.

Prerequisites: the full dev stack must be running.
    make dev && make bootstrap

All tests in this directory are automatically skipped when the serving
API or Kafka is not reachable, so they are safe to run in CI as long as
the stack is not started there.
"""
import time
import psycopg2
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import pytest
import requests

try:
    from confluent_kafka import Producer as _KafkaProducer
    _CONFLUENT_AVAILABLE = True
except ImportError:
    _CONFLUENT_AVAILABLE = False

from urbanpulse_core.models.traffic import CongestionMetrics, TrafficRouteObservation

KAFKA_BOOTSTRAP = "localhost:19092"
SERVING_BASE    = "http://localhost:8001"
KAFKA_TOPIC     = "traffic-route-bronze"


# ── connectivity probes ───────────────────────────────────────────────────────

def _serving_live() -> bool:
    try:
        r = requests.get(f"{SERVING_BASE}/health/live", timeout=3)
        return r.status_code == 200
    except Exception:
        return False


def _kafka_reachable() -> bool:
    if not _CONFLUENT_AVAILABLE:
        return False
    try:
        p = _KafkaProducer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "socket.timeout.ms": 3000,
            "message.timeout.ms": 3000,
        })
        p.flush(timeout=3)
        return True
    except Exception:
        return False


# ── module-scoped skip fixtures ───────────────────────────────────────────────

@pytest.fixture(scope="session", autouse=True)
def cleanup_smoke_routes():
    """Delete all smoke_test_* routes from Postgres after the test session.

    Integration tests publish to the real Kafka topic, so the online service
    writes smoke observations into the live DB. This fixture cleans up after
    the entire session so test data never pollutes production metrics.
    """
    yield  # let all tests run first

    from urbanpulse_core.config import settings
    try:
        conn = psycopg2.connect(settings.postgres_dsn)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("DELETE FROM online_route_features WHERE route_id LIKE 'smoke_%'")
            cur.execute("DELETE FROM route_iforest_scores  WHERE route_id LIKE 'smoke_%'")
            cur.execute("DELETE FROM prediction_history    WHERE route_id LIKE 'smoke_%'")
        conn.close()
    except Exception as exc:
        # Non-fatal: stack may not be running (CI without dev stack)
        print(f"\n[conftest] smoke cleanup skipped: {exc}")


@pytest.fixture(scope="module")
def require_serving():
    """Skip the test module when the serving API is not reachable."""
    if not _serving_live():
        pytest.skip("Serving API not reachable — run: make dev")


@pytest.fixture(scope="module")
def require_kafka():
    """Skip when Kafka/Redpanda is not reachable."""
    if not _kafka_reachable():
        pytest.skip("Kafka not reachable — run: make dev")


# ── Kafka publish helper ──────────────────────────────────────────────────────

class KafkaPublisher:
    def __init__(self) -> None:
        self._producer = _KafkaProducer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "socket.timeout.ms": 5000,
            "message.timeout.ms": 5000,
        })

    def publish(self, obs: TrafficRouteObservation) -> None:
        poll_ts_ms = int(time.time() * 1000)
        payload = obs.model_dump_json().encode()
        self._producer.produce(
            KAFKA_TOPIC,
            key=obs.route_id.encode(),
            value=payload,
            headers={"ingest_ts": str(poll_ts_ms).encode()},
        )
        self._producer.flush(timeout=10)


@pytest.fixture(scope="module")
def kafka_publisher(require_kafka):
    return KafkaPublisher()


# ── observation factory ───────────────────────────────────────────────────────

def make_smoke_observation(
    route_id: str,
    duration_minutes: float = 12.0,
    heavy_ratio: float = 0.15,
    severe_segments: int = 0,
) -> TrafficRouteObservation:
    return TrafficRouteObservation(
        route_id=route_id,
        origin="Smoke Origin",
        destination="Smoke Destination",
        distance_meters=8000.0,
        duration_ms=duration_minutes * 60_000,
        duration_minutes=duration_minutes,
        congestion=CongestionMetrics(
            heavy_ratio=heavy_ratio,
            moderate_ratio=0.3,
            low_ratio=max(0.0, 1.0 - heavy_ratio - 0.3),
            severe_segments=severe_segments,
            total_segments=20,
        ),
        timestamp_utc=datetime.now(timezone.utc),
    )


# ── polling helper ────────────────────────────────────────────────────────────

def poll_until(
    url: str,
    condition: Any,
    timeout_s: int = 60,
    interval_s: float = 2.0,
) -> requests.Response | None:
    """GET url repeatedly until condition(response) is True or timeout."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            r = requests.get(url, timeout=5)
            if condition(r):
                return r
        except Exception:
            pass
        time.sleep(interval_s)
    return None


def unique_route_id() -> str:
    return f"smoke_test_{uuid4().hex[:8]}"
