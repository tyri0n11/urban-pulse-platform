"""Tests for OnlineFeatureProcessor.process() — mocks out Postgres and Iceberg."""
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from online.baseline import BaselineEntry
from online.models import RouteWindow
from conftest import make_kafka_msg


def _make_processor(baseline: dict | None = None):
    """Return an OnlineFeatureProcessor with all infrastructure mocked out."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_conn.cursor.return_value.__exit__.return_value = None

    from online.app import OnlineFeatureProcessor

    with (
        patch("online.app.psycopg2.connect", return_value=mock_conn),
        patch("online.app.load_baseline", return_value=baseline or {}),
    ):
        proc = OnlineFeatureProcessor(pg_dsn="mock://test")

    return proc, mock_cursor


@pytest.mark.unit
class TestProcessBuffering:
    def test_first_message_creates_window(self, sample_obs):
        proc, _ = _make_processor()
        msg = make_kafka_msg(sample_obs)
        proc.process(msg)
        assert sample_obs.route_id in proc._windows

    def test_second_message_increments_count(self, sample_obs):
        proc, _ = _make_processor()
        proc.process(make_kafka_msg(sample_obs))
        proc.process(make_kafka_msg(sample_obs))
        assert proc._windows[sample_obs.route_id].count == 2

    def test_new_hour_resets_window(self, sample_obs):
        proc, _ = _make_processor()
        # Plant an old window from a different hour
        old_ts = 0  # epoch — far from current hour
        proc._windows[sample_obs.route_id] = RouteWindow(window_start_ts=old_ts, count=99)

        proc.process(make_kafka_msg(sample_obs))
        # Window should be reset (count starts fresh)
        assert proc._windows[sample_obs.route_id].count == 1

    def test_invalid_json_does_not_raise(self):
        proc, _ = _make_processor()
        msg = MagicMock()
        msg.value.return_value = b"not-json"
        msg.headers.return_value = []
        proc.process(msg)  # must not raise

    def test_missing_ingest_ts_header_defaults_to_zero_lag(self, sample_obs):
        proc, _ = _make_processor()
        msg = make_kafka_msg(sample_obs, ingest_ts_ms=None)
        proc.process(msg)
        assert proc._windows[sample_obs.route_id].last_ingest_lag_ms == 0


@pytest.mark.unit
class TestProcessZScore:
    def test_no_baseline_zscore_is_none(self, sample_obs):
        proc, mock_cursor = _make_processor(baseline={})
        proc.process(make_kafka_msg(sample_obs))
        _, kwargs = mock_cursor.execute.call_args_list[-1]
        # duration_zscore passed to upsert should be None
        params: dict = mock_cursor.execute.call_args_list[-1][0][1]
        assert params["duration_zscore"] is None
        assert params["is_anomaly"] is False

    def test_zscore_anomaly_when_heavy_ratio_above_threshold(self, sample_obs):
        """is_anomaly=True when mean_heavy_ratio deviates beyond zscore_threshold."""
        baseline = {
            sample_obs.route_id: BaselineEntry(
                mean=10.0,
                stddev=1.0,
                heavy_ratio_mean=0.05,   # baseline: 5% heavy
                heavy_ratio_stddev=0.02,
                zscore_threshold=2.0,
            )
        }
        proc, mock_cursor = _make_processor(baseline=baseline)

        # heavy_ratio=0.2 → zscore = (0.2-0.05)/0.02 = 7.5 > 2.0
        proc.process(make_kafka_msg(sample_obs))
        params: dict = mock_cursor.execute.call_args_list[-1][0][1]
        assert params["is_anomaly"] is True

    def test_no_anomaly_when_heavy_ratio_within_threshold(self, sample_obs):
        baseline = {
            sample_obs.route_id: BaselineEntry(
                mean=10.0,
                stddev=1.0,
                heavy_ratio_mean=0.18,   # baseline close to observed 0.2
                heavy_ratio_stddev=0.05,
                zscore_threshold=2.0,
            )
        }
        proc, mock_cursor = _make_processor(baseline=baseline)

        # zscore = (0.2-0.18)/0.05 = 0.4 < 2.0
        proc.process(make_kafka_msg(sample_obs))
        params: dict = mock_cursor.execute.call_args_list[-1][0][1]
        assert params["is_anomaly"] is False

    def test_upsert_called_with_route_id(self, sample_obs):
        proc, mock_cursor = _make_processor()
        proc.process(make_kafka_msg(sample_obs))
        params: dict = mock_cursor.execute.call_args_list[-1][0][1]
        assert params["route_id"] == sample_obs.route_id

    def test_upsert_observation_count_matches_window(self, sample_obs):
        proc, mock_cursor = _make_processor()
        proc.process(make_kafka_msg(sample_obs))
        proc.process(make_kafka_msg(sample_obs))
        params: dict = mock_cursor.execute.call_args_list[-1][0][1]
        assert params["observation_count"] == 2
