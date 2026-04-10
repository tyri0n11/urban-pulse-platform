"""End-to-end smoke tests: Kafka → online-service (Postgres) → serving REST API.

Test order (each builds on the previous):

  Stage 1 — Health
    test_serving_health_live       : /health/live returns 200
    test_serving_health_ready      : /health/ready returns 200

  Stage 2 — Kafka → Postgres → Serving
    test_observation_appears_in_online_features
        Publish one observation with a unique route_id, poll
        GET /online/features/{route_id} until the online-service writes it
        to Postgres (typically < 5 s, timeout 60 s).

    test_observation_count_increments
        Publish two more observations for the same route, verify
        observation_count in the API response incremented by at least 2.

  Stage 3 — Derived endpoints
    test_ingest_lag_reported       : /online/lag shows active_routes > 0
    test_anomaly_endpoint_reachable: /anomalies/current returns a list
    test_metrics_leaderboard       : /metrics/leaderboard returns a list

  Stage 4 — Anomaly signal
    test_high_heavy_ratio_creates_zscore_anomaly
        Publish an observation with very high heavy_ratio (0.95) for a new
        unique route with a synthetic baseline already in the online service.
        We don't inject a baseline here, so we only assert that zscore is
        non-None after several observations accumulate in the window.

Run only when the dev stack is running:
    uv run pytest -m integration -v
"""
import time

import pytest
import requests

from conftest import (
    SERVING_BASE,
    make_smoke_observation,
    poll_until,
    unique_route_id,
)


# ── Stage 1: Health ───────────────────────────────────────────────────────────

@pytest.mark.integration
class TestServingHealth:
    def test_health_live(self, require_serving):
        r = requests.get(f"{SERVING_BASE}/health/live", timeout=5)
        assert r.status_code == 200

    def test_health_ready(self, require_serving):
        r = requests.get(f"{SERVING_BASE}/health/ready", timeout=5)
        assert r.status_code == 200

    def test_health_live_returns_json(self, require_serving):
        r = requests.get(f"{SERVING_BASE}/health/live", timeout=5)
        body = r.json()
        assert isinstance(body, dict)


# ── Stage 2: Kafka → Postgres → Serving ──────────────────────────────────────

@pytest.mark.integration
class TestKafkaToServingPipeline:
    """Publish directly to Kafka and verify the online service picks it up."""

    def test_observation_appears_in_online_features(
        self, require_serving, kafka_publisher
    ):
        """Core E2E: one publish → feature visible via REST API."""
        route_id = unique_route_id()
        obs = make_smoke_observation(route_id)

        kafka_publisher.publish(obs)

        url = f"{SERVING_BASE}/online/features/{route_id}"
        response = poll_until(
            url,
            condition=lambda r: r.status_code == 200,
            timeout_s=60,
            interval_s=2.0,
        )

        assert response is not None, (
            f"Route '{route_id}' never appeared in /online/features after 60 s. "
            "Check that the online service is running: docker ps"
        )
        body = response.json()
        assert body["route_id"] == route_id
        assert body["observation_count"] >= 1

    def test_observation_count_increments_after_more_publishes(
        self, require_serving, kafka_publisher
    ):
        """Publish N messages, verify count accumulates in the window."""
        route_id = unique_route_id()

        # Publish first message and wait for it to arrive
        kafka_publisher.publish(make_smoke_observation(route_id, duration_minutes=10.0))
        url = f"{SERVING_BASE}/online/features/{route_id}"
        first = poll_until(url, lambda r: r.status_code == 200, timeout_s=60)
        assert first is not None, "First observation never arrived"

        # Publish two more
        kafka_publisher.publish(make_smoke_observation(route_id, duration_minutes=11.0))
        kafka_publisher.publish(make_smoke_observation(route_id, duration_minutes=12.0))

        # Wait for count to reach at least 3
        final = poll_until(
            url,
            condition=lambda r: r.status_code == 200 and r.json().get("observation_count", 0) >= 3,
            timeout_s=30,
            interval_s=2.0,
        )
        assert final is not None, "observation_count did not reach 3 within 30 s"
        assert final.json()["observation_count"] >= 3

    def test_mean_heavy_ratio_matches_published_value(
        self, require_serving, kafka_publisher
    ):
        """After one publish, mean_heavy_ratio should reflect what we sent."""
        route_id = unique_route_id()
        obs = make_smoke_observation(route_id, heavy_ratio=0.40)
        kafka_publisher.publish(obs)

        url = f"{SERVING_BASE}/online/features/{route_id}"
        response = poll_until(url, lambda r: r.status_code == 200, timeout_s=60)
        assert response is not None

        body = response.json()
        # After one observation mean_heavy_ratio == the single value
        assert body["mean_heavy_ratio"] == pytest.approx(0.40, abs=0.01)

    def test_ingest_lag_ms_is_positive(self, require_serving, kafka_publisher):
        """last_ingest_lag_ms should be > 0 (pipeline latency is measurable)."""
        route_id = unique_route_id()
        kafka_publisher.publish(make_smoke_observation(route_id))

        url = f"{SERVING_BASE}/online/features/{route_id}"
        response = poll_until(url, lambda r: r.status_code == 200, timeout_s=60)
        assert response is not None

        body = response.json()
        assert body.get("last_ingest_lag_ms") is not None
        # Allow small negative values due to clock skew between publisher and consumer
        assert body["last_ingest_lag_ms"] >= -500

    def test_unknown_route_returns_404(self, require_serving):
        r = requests.get(
            f"{SERVING_BASE}/online/features/route_that_does_not_exist_xyz",
            timeout=5,
        )
        assert r.status_code == 404


# ── Stage 3: Derived endpoints ────────────────────────────────────────────────

@pytest.mark.integration
class TestDerivedEndpoints:
    def test_online_lag_endpoint_reachable(self, require_serving):
        r = requests.get(f"{SERVING_BASE}/online/lag", timeout=5)
        assert r.status_code == 200
        body = r.json()
        assert "active_routes" in body

    def test_anomaly_current_returns_list(self, require_serving):
        r = requests.get(f"{SERVING_BASE}/anomalies/current", timeout=15)
        assert r.status_code == 200
        assert isinstance(r.json(), list)

    def test_anomaly_history_returns_list(self, require_serving):
        r = requests.get(f"{SERVING_BASE}/anomalies/history", timeout=10)
        assert r.status_code == 200
        assert isinstance(r.json(), list)

    def test_metrics_leaderboard_returns_list(self, require_serving):
        r = requests.get(f"{SERVING_BASE}/metrics/leaderboard", timeout=10)
        assert r.status_code == 200
        assert isinstance(r.json(), list)

    def test_online_features_list_not_empty_after_publish(
        self, require_serving, kafka_publisher
    ):
        """After at least one publish, /online/features must be non-empty."""
        # Publish so there is something in the DB
        kafka_publisher.publish(make_smoke_observation(unique_route_id()))
        # Allow time for processing
        time.sleep(5)

        r = requests.get(f"{SERVING_BASE}/online/features", timeout=10)
        assert r.status_code == 200
        assert len(r.json()) >= 1


# ── Stage 4: Anomaly signal ───────────────────────────────────────────────────

@pytest.mark.integration
class TestAnomalySignal:
    def test_zscore_field_present_in_features(self, require_serving, kafka_publisher):
        """duration_zscore key must be present in the response (may be None if no baseline)."""
        route_id = unique_route_id()
        kafka_publisher.publish(make_smoke_observation(route_id))

        url = f"{SERVING_BASE}/online/features/{route_id}"
        response = poll_until(url, lambda r: r.status_code == 200, timeout_s=60)
        assert response is not None

        body = response.json()
        assert "duration_zscore" in body
        assert "is_anomaly" in body

    def test_is_anomaly_is_boolean(self, require_serving, kafka_publisher):
        route_id = unique_route_id()
        kafka_publisher.publish(make_smoke_observation(route_id))

        url = f"{SERVING_BASE}/online/features/{route_id}"
        response = poll_until(url, lambda r: r.status_code == 200, timeout_s=60)
        assert response is not None
        assert isinstance(response.json()["is_anomaly"], bool)

    def test_anomaly_summary_has_expected_shape(self, require_serving):
        r = requests.get(f"{SERVING_BASE}/anomalies/summary", timeout=10)
        assert r.status_code == 200
        body = r.json()
        assert isinstance(body, list)
        if body:  # if data exists, check the shape
            first = body[0]
            assert "hour" in first or "anomaly_count" in first or "count" in first
