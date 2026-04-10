"""Tests for prediction_service — feature vector building and score_rows."""
import math
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from serving.services.prediction_service import (
    _build_feature_vector,
    score_rows,
)


# ── _build_feature_vector ─────────────────────────────────────────────────────

@pytest.mark.unit
class TestBuildFeatureVector:
    def _row(self, **kwargs):
        defaults = {
            "route_id": "zone1_test",
            "mean_heavy_ratio": 0.2,
            "mean_moderate_ratio": 0.3,
            "max_severe_segments": 2.0,
            "is_anomaly": False,
            "window_start": datetime(2026, 4, 10, 8, 0, 0, tzinfo=timezone.utc),
        }
        defaults.update(kwargs)
        return defaults

    def test_output_length_is_seven(self):
        vec = _build_feature_vector(self._row())
        assert len(vec) == 7

    def test_heavy_ratio_at_index_zero(self):
        vec = _build_feature_vector(self._row(mean_heavy_ratio=0.4))
        assert vec[0] == pytest.approx(0.4)

    def test_moderate_ratio_at_index_one(self):
        vec = _build_feature_vector(self._row(mean_moderate_ratio=0.25))
        assert vec[1] == pytest.approx(0.25)

    def test_max_severe_at_index_two(self):
        vec = _build_feature_vector(self._row(max_severe_segments=3.0))
        assert vec[2] == pytest.approx(3.0)

    def test_hour_encoding_is_correct(self):
        # window_start hour=6 → hour_sin=1, hour_cos≈0
        row = self._row(window_start=datetime(2026, 4, 10, 6, 0, 0, tzinfo=timezone.utc))
        vec = _build_feature_vector(row)
        assert vec[3] == pytest.approx(math.sin(2 * math.pi * 6 / 24), abs=1e-9)
        assert vec[4] == pytest.approx(math.cos(2 * math.pi * 6 / 24), abs=1e-9)

    def test_dow_encoding_for_friday(self):
        # 2026-04-10 is Friday (weekday=4) → SQL DOW = (4+1)%7 = 5
        row = self._row(window_start=datetime(2026, 4, 10, 8, 0, 0, tzinfo=timezone.utc))
        dow = (datetime(2026, 4, 10).weekday() + 1) % 7
        vec = _build_feature_vector(row)
        assert vec[5] == pytest.approx(math.sin(2 * math.pi * dow / 7), abs=1e-9)
        assert vec[6] == pytest.approx(math.cos(2 * math.pi * dow / 7), abs=1e-9)

    def test_missing_window_start_defaults_to_zero_time(self):
        row = self._row()
        row["window_start"] = None
        vec = _build_feature_vector(row)
        # hour=0, dow=0 → hour_sin=0, hour_cos=1, dow_sin=0, dow_cos=1
        assert vec[3] == pytest.approx(0.0, abs=1e-9)  # hour_sin
        assert vec[4] == pytest.approx(1.0, abs=1e-9)  # hour_cos

    def test_none_values_default_to_zero(self):
        row = self._row(mean_heavy_ratio=None, mean_moderate_ratio=None, max_severe_segments=None)
        vec = _build_feature_vector(row)
        assert vec[0] == pytest.approx(0.0)
        assert vec[1] == pytest.approx(0.0)
        assert vec[2] == pytest.approx(0.0)

    def test_output_is_float64_numpy_array(self):
        vec = _build_feature_vector(self._row())
        assert isinstance(vec, np.ndarray)
        assert vec.dtype == np.float64


# ── score_rows ────────────────────────────────────────────────────────────────

@pytest.mark.unit
class TestScoreRows:
    def _make_row(self, route_id: str = "route_a", is_anomaly: bool = False):
        return {
            "route_id": route_id,
            "mean_heavy_ratio": 0.1,
            "mean_moderate_ratio": 0.2,
            "max_severe_segments": 0.0,
            "is_anomaly": is_anomaly,
            "window_start": datetime(2026, 4, 10, 8, 0, 0, tzinfo=timezone.utc),
        }

    def test_empty_input_returns_empty(self):
        assert score_rows([]) == []

    def test_returns_one_result_per_row(self):
        rows = [self._make_row("r1"), self._make_row("r2")]
        mock_model = MagicMock()
        mock_model.predict.return_value = np.array([1])
        mock_model.decision_function.return_value = np.array([-0.1])
        with patch(
            "serving.services.prediction_service._get_route_cache"
        ) as mock_cache:
            entry = MagicMock()
            entry.model = mock_model
            mock_cache.return_value = entry
            results = score_rows(rows)
        assert len(results) == 2

    def test_zscore_anomaly_from_is_anomaly_field(self):
        rows = [self._make_row("r1", is_anomaly=True)]
        mock_model = MagicMock()
        mock_model.predict.return_value = np.array([1])
        mock_model.decision_function.return_value = np.array([-0.1])
        with patch("serving.services.prediction_service._get_route_cache") as mock_cache:
            entry = MagicMock()
            entry.model = mock_model
            mock_cache.return_value = entry
            results = score_rows(rows)
        assert results[0].zscore_anomaly is True

    def test_both_anomaly_requires_both_signals(self):
        rows = [self._make_row("r1", is_anomaly=True)]
        mock_model = MagicMock()
        mock_model.predict.return_value = np.array([-1])  # sklearn: -1 = anomaly
        mock_model.decision_function.return_value = np.array([-0.2])
        with patch("serving.services.prediction_service._get_route_cache") as mock_cache:
            entry = MagicMock()
            entry.model = mock_model
            mock_cache.return_value = entry
            results = score_rows(rows)
        assert results[0].iforest_anomaly is True
        assert results[0].zscore_anomaly is True
        assert results[0].both_anomaly is True

    def test_no_model_falls_back_to_non_anomaly(self):
        """Routes with no trained model default to iforest_anomaly=False."""
        rows = [self._make_row("route_unknown")]
        with patch(
            "serving.services.prediction_service._get_route_cache",
            side_effect=RuntimeError("no model"),
        ):
            results = score_rows(rows)
        assert len(results) == 1
        assert results[0].iforest_anomaly is False
        assert results[0].iforest_score == 0.0

    def test_result_route_id_matches_input(self):
        rows = [self._make_row("zone3_northern_to_zone5_western")]
        with patch(
            "serving.services.prediction_service._get_route_cache",
            side_effect=RuntimeError("no model"),
        ):
            results = score_rows(rows)
        assert results[0].route_id == "zone3_northern_to_zone5_western"
