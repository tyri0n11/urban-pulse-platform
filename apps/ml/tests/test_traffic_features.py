"""Tests for cyclical_time_features and FEATURE_COLUMNS definition."""
import math

import pytest

from ml.features.traffic_features import FEATURE_COLUMNS, cyclical_time_features


@pytest.mark.unit
class TestCyclicalTimeFeatures:
    def test_hour_zero_sin_is_zero_cos_is_one(self):
        h_sin, h_cos, _, _ = cyclical_time_features(hour=0, dow=0)
        assert h_sin == pytest.approx(0.0, abs=1e-9)
        assert h_cos == pytest.approx(1.0, abs=1e-9)

    def test_hour_6_sin_is_one_cos_is_zero(self):
        # 6/24 of period → sin=1, cos=0
        h_sin, h_cos, _, _ = cyclical_time_features(hour=6, dow=0)
        assert h_sin == pytest.approx(1.0, abs=1e-9)
        assert h_cos == pytest.approx(0.0, abs=1e-9)

    def test_hour_12_cos_is_minus_one(self):
        # half period → cos = -1
        h_sin, h_cos, _, _ = cyclical_time_features(hour=12, dow=0)
        assert h_cos == pytest.approx(-1.0, abs=1e-9)

    def test_hour_periodicity_24_equals_0(self):
        # hour=24 should not be used, but sanity: 24 mod 24 = 0
        h_sin_0, h_cos_0, _, _ = cyclical_time_features(hour=0, dow=0)
        h_sin_24 = math.sin(2 * math.pi * 24 / 24)
        h_cos_24 = math.cos(2 * math.pi * 24 / 24)
        assert h_sin_24 == pytest.approx(h_sin_0, abs=1e-9)
        assert h_cos_24 == pytest.approx(h_cos_0, abs=1e-9)

    def test_dow_zero_sin_is_zero_cos_is_one(self):
        _, _, d_sin, d_cos = cyclical_time_features(hour=0, dow=0)
        assert d_sin == pytest.approx(0.0, abs=1e-9)
        assert d_cos == pytest.approx(1.0, abs=1e-9)

    def test_all_values_in_minus_one_to_one(self):
        for h in range(24):
            for d in range(7):
                vals = cyclical_time_features(hour=h, dow=d)
                for v in vals:
                    assert -1.0 - 1e-9 <= v <= 1.0 + 1e-9

    def test_returns_four_floats(self):
        result = cyclical_time_features(hour=8, dow=1)
        assert len(result) == 4
        for v in result:
            assert isinstance(v, float)

    def test_different_hours_produce_different_vectors(self):
        v1 = cyclical_time_features(hour=8, dow=1)
        v2 = cyclical_time_features(hour=9, dow=1)
        assert v1 != v2


@pytest.mark.unit
class TestFeatureColumns:
    def test_feature_columns_has_seven_entries(self):
        assert len(FEATURE_COLUMNS) == 7

    def test_required_columns_present(self):
        required = {
            "avg_heavy_ratio", "avg_moderate_ratio", "max_severe_segments",
            "hour_sin", "hour_cos", "dow_sin", "dow_cos",
        }
        assert set(FEATURE_COLUMNS) == required
