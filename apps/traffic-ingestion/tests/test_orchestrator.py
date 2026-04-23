"""Tests for the ingestion orchestrator."""
from unittest.mock import MagicMock, patch

import pytest


@pytest.mark.unit
class TestOrchestratorRun:
    def test_publishes_once_per_route(self, sample_routes, sample_observation):
        """run() calls publisher.publish exactly N times for N routes."""
        from traffic_ingestion.orchestrator import run

        publisher = MagicMock()
        with (
            patch("ingestion.orchestrator._load_routes", return_value=sample_routes),
            patch("ingestion.orchestrator.fetch_route", return_value=sample_observation),
            patch("time.sleep"),
        ):
            run(publisher, api_key="test-key")

        assert publisher.publish.call_count == len(sample_routes)

    def test_skips_failed_route_continues_rest(self, sample_routes, sample_observation):
        """If one route raises, orchestrator logs and moves on to the next."""
        from traffic_ingestion.orchestrator import run

        publisher = MagicMock()
        with (
            patch("ingestion.orchestrator._load_routes", return_value=sample_routes),
            patch(
                "ingestion.orchestrator.fetch_route",
                side_effect=[Exception("API timeout"), sample_observation],
            ),
            patch("time.sleep"),
        ):
            run(publisher, api_key="test-key")

        assert publisher.publish.call_count == 1

    def test_empty_routes_never_calls_publish(self):
        """With no routes, publisher is never called."""
        from traffic_ingestion.orchestrator import run

        publisher = MagicMock()
        with patch("ingestion.orchestrator._load_routes", return_value=[]):
            run(publisher, api_key="test-key")

        publisher.publish.assert_not_called()

    def test_api_key_forwarded_to_every_fetch(self, sample_routes, sample_observation):
        """The api_key must reach every fetch_route call."""
        from traffic_ingestion.orchestrator import run

        publisher = MagicMock()
        with (
            patch("ingestion.orchestrator._load_routes", return_value=sample_routes),
            patch(
                "ingestion.orchestrator.fetch_route", return_value=sample_observation
            ) as mock_fetch,
            patch("time.sleep"),
        ):
            run(publisher, api_key="secret-key")

        for call in mock_fetch.call_args_list:
            assert call.kwargs["api_key"] == "secret-key"

    def test_poll_ts_ms_passed_to_publish(self, sample_routes, sample_observation):
        """publish() must receive a non-None poll_ts_ms (wall-clock before API call)."""
        from traffic_ingestion.orchestrator import run

        publisher = MagicMock()
        with (
            patch("ingestion.orchestrator._load_routes", return_value=sample_routes),
            patch("ingestion.orchestrator.fetch_route", return_value=sample_observation),
            patch("time.sleep"),
        ):
            run(publisher, api_key="key")

        for call in publisher.publish.call_args_list:
            assert call.kwargs.get("poll_ts_ms") is not None

    def test_sleep_between_routes_not_after_last(self, sample_routes, sample_observation):
        """sleep is called N-1 times (between routes, not after the last one)."""
        from traffic_ingestion.orchestrator import run

        publisher = MagicMock()
        with (
            patch("ingestion.orchestrator._load_routes", return_value=sample_routes),
            patch("ingestion.orchestrator.fetch_route", return_value=sample_observation),
            patch("time.sleep") as mock_sleep,
        ):
            run(publisher, api_key="key")

        assert mock_sleep.call_count == len(sample_routes) - 1
