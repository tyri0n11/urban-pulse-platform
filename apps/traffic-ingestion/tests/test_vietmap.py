"""Tests for the VietMap source connector."""
from unittest.mock import MagicMock, patch

import pytest
import requests

from traffic_ingestion.sources.vietmap import fetch_route_raw
from urbanpulse_core.models.traffic import (
    CongestionMetrics,
    VietmapRawEnvelope,
    _calc_congestion,
)


def _make_api_response(
    distance: float = 5000.0,
    duration_ms: float = 300000.0,
    congestion_segs: list[dict] | None = None,
) -> MagicMock:
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json.return_value = {
        "paths": [
            {
                "distance": distance,
                "time": duration_ms,
                "annotations": {"congestion": congestion_segs or []},
            }
        ]
    }
    return mock_resp


# ── _calc_congestion ──────────────────────────────────────────────────────────

@pytest.mark.unit
class TestCalcCongestion:
    def test_empty_segments_returns_zero_metrics(self):
        result = _calc_congestion([])
        assert result == CongestionMetrics()

    def test_all_heavy_ratio_is_one(self):
        segs = [{"value": "heavy"}] * 4
        result = _calc_congestion(segs)
        assert result.heavy_ratio == 1.0
        assert result.moderate_ratio == 0.0
        assert result.total_segments == 4

    def test_mixed_segments_correct_ratios(self):
        segs = [
            {"value": "heavy"},
            {"value": "heavy"},
            {"value": "moderate"},
            {"value": "low"},
            {"value": "unknown"},
        ]
        result = _calc_congestion(segs)
        assert result.heavy_ratio == pytest.approx(0.4)
        assert result.moderate_ratio == pytest.approx(0.2)
        assert result.low_ratio == pytest.approx(0.2)
        assert result.total_segments == 5

    def test_severe_counted_separately_not_in_heavy(self):
        segs = [{"value": "severe"}, {"value": "severe"}, {"value": "low"}]
        result = _calc_congestion(segs)
        assert result.severe_segments == 2
        assert result.heavy_ratio == 0.0
        assert result.total_segments == 3

    def test_unknown_value_ignored_in_ratios(self):
        segs = [{"value": "unknown"}, {"value": "unknown"}]
        result = _calc_congestion(segs)
        assert result.heavy_ratio == 0.0
        assert result.moderate_ratio == 0.0
        assert result.total_segments == 2

    def test_ratios_rounded_to_3_decimal_places(self):
        segs = [{"value": "heavy"}] * 1 + [{"value": "low"}] * 2
        result = _calc_congestion(segs)
        assert result.heavy_ratio == pytest.approx(0.333, abs=1e-3)


# ── fetch_route_raw ───────────────────────────────────────────────────────────

@pytest.mark.unit
class TestFetchRouteRaw:
    def _fetch(self, mock_resp: MagicMock, **kwargs):
        defaults = dict(
            route_id="zone1_to_zone4",
            origin="A",
            destination="B",
            origin_anchor=[10.78, 106.70],
            destination_anchor=[10.73, 106.72],
            api_key="key123",
        )
        defaults.update(kwargs)
        with patch("traffic_ingestion.sources.vietmap.requests.get", return_value=mock_resp):
            envelope, polled_at_ms = fetch_route_raw(**defaults)
        return envelope, polled_at_ms

    def test_returns_envelope_and_timestamp(self):
        resp = _make_api_response(distance=12500.0, duration_ms=900000.0)
        envelope, polled_at_ms = self._fetch(resp)
        assert isinstance(envelope, VietmapRawEnvelope)
        assert isinstance(polled_at_ms, int)

    def test_envelope_route_id_set_correctly(self):
        envelope, _ = self._fetch(_make_api_response())
        assert envelope.route_id == "zone1_to_zone4"

    def test_envelope_origin_destination_set(self):
        envelope, _ = self._fetch(_make_api_response())
        assert envelope.origin == "A"
        assert envelope.destination == "B"

    def test_polled_at_ms_matches_envelope(self):
        envelope, polled_at_ms = self._fetch(_make_api_response())
        assert envelope.polled_at_ms == polled_at_ms

    def test_raw_response_preserved(self):
        resp = _make_api_response(
            distance=5000.0,
            congestion_segs=[{"value": "heavy"}, {"value": "low"}],
        )
        envelope, _ = self._fetch(resp)
        assert "paths" in envelope.raw_response
        assert envelope.raw_response["paths"][0]["distance"] == 5000.0

    def test_raises_on_http_error(self):
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = requests.HTTPError("404")
        with pytest.raises(requests.HTTPError):
            self._fetch(mock_resp)

    def test_api_key_included_in_request_url(self):
        resp = _make_api_response()
        with patch(
            "traffic_ingestion.sources.vietmap.requests.get", return_value=resp
        ) as mock_get:
            fetch_route_raw(
                route_id="r",
                origin="A",
                destination="B",
                origin_anchor=[10.0, 106.0],
                destination_anchor=[10.1, 106.1],
                api_key="super-secret",
            )
        url: str = mock_get.call_args[0][0]
        assert "super-secret" in url

    def test_anchor_coordinates_in_request_url(self):
        resp = _make_api_response()
        with patch(
            "traffic_ingestion.sources.vietmap.requests.get", return_value=resp
        ) as mock_get:
            fetch_route_raw(
                route_id="r",
                origin="A",
                destination="B",
                origin_anchor=[10.78, 106.70],
                destination_anchor=[10.73, 106.72],
                api_key="key",
            )
        url: str = mock_get.call_args[0][0]
        assert "10.78" in url
        assert "106.7" in url
