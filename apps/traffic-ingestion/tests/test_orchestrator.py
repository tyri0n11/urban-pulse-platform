"""Tests for the ingestion orchestrator."""
from unittest.mock import MagicMock, patch

import pytest


def _mock_fetch_raw(envelope):
    """Return a side_effect-compatible callable that yields (envelope, polled_at_ms)."""
    return (envelope, envelope.polled_at_ms)


@pytest.mark.unit
class TestOrchestratorRun:
    def test_publishes_once_per_route(self, sample_routes, sample_envelope):
        """run() calls publisher.publish exactly N times for N routes."""
        from traffic_ingestion.orchestrator import run

        publisher = MagicMock()
        with (
            patch("traffic_ingestion.orchestrator._load_routes", return_value=sample_routes),
            patch(
                "traffic_ingestion.orchestrator.fetch_route_raw",
                return_value=(sample_envelope, sample_envelope.polled_at_ms),
            ),
            patch("time.sleep"),
        ):
            run(publisher, api_key="test-key")

        assert publisher.publish.call_count == len(sample_routes)

    def test_skips_failed_route_continues_rest(self, sample_routes, sample_envelope):
        """If one route raises, orchestrator logs and moves on to the next."""
        from traffic_ingestion.orchestrator import run

        publisher = MagicMock()
        with (
            patch("traffic_ingestion.orchestrator._load_routes", return_value=sample_routes),
            patch(
                "traffic_ingestion.orchestrator.fetch_route_raw",
                side_effect=[
                    Exception("API timeout"),
                    (sample_envelope, sample_envelope.polled_at_ms),
                ],
            ),
            patch("time.sleep"),
        ):
            run(publisher, api_key="test-key")

        assert publisher.publish.call_count == 1

    def test_empty_routes_never_calls_publish(self):
        """With no routes, publisher is never called."""
        from traffic_ingestion.orchestrator import run

        publisher = MagicMock()
        with patch("traffic_ingestion.orchestrator._load_routes", return_value=[]):
            run(publisher, api_key="test-key")

        publisher.publish.assert_not_called()

    def test_api_key_forwarded_to_every_fetch(self, sample_routes, sample_envelope):
        """The api_key must reach every fetch_route_raw call."""
        from traffic_ingestion.orchestrator import run

        publisher = MagicMock()
        with (
            patch("traffic_ingestion.orchestrator._load_routes", return_value=sample_routes),
            patch(
                "traffic_ingestion.orchestrator.fetch_route_raw",
                return_value=(sample_envelope, sample_envelope.polled_at_ms),
            ) as mock_fetch,
            patch("time.sleep"),
        ):
            run(publisher, api_key="secret-key")

        for call in mock_fetch.call_args_list:
            assert call.kwargs["api_key"] == "secret-key"

    def test_sleep_between_routes_not_after_last(self, sample_routes, sample_envelope):
        """sleep is called N-1 times (between routes, not after the last one)."""
        from traffic_ingestion.orchestrator import run

        publisher = MagicMock()
        with (
            patch("traffic_ingestion.orchestrator._load_routes", return_value=sample_routes),
            patch(
                "traffic_ingestion.orchestrator.fetch_route_raw",
                return_value=(sample_envelope, sample_envelope.polled_at_ms),
            ),
            patch("time.sleep") as mock_sleep,
        ):
            run(publisher, api_key="key")

        assert mock_sleep.call_count == len(sample_routes) - 1
