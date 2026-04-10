"""Tests for RouteWindow — Welford's online algorithm and derived properties."""
import pytest

from online.models import RouteWindow


@pytest.mark.unit
class TestRouteWindowUpdate:
    def test_first_update_sets_mean_to_value(self):
        w = RouteWindow()
        w.update(10.0, 0.2, 0.3, 0.5, 0.0, 0)
        assert w.mean_duration == pytest.approx(10.0)
        assert w.count == 1

    def test_two_updates_mean_is_average(self):
        w = RouteWindow()
        w.update(10.0, 0.0, 0.0, 1.0, 0.0, 0)
        w.update(20.0, 0.0, 0.0, 1.0, 0.0, 0)
        assert w.mean_duration == pytest.approx(15.0)
        assert w.count == 2

    def test_stddev_zero_for_single_update(self):
        w = RouteWindow()
        w.update(10.0, 0.0, 0.0, 1.0, 0.0, 0)
        assert w.stddev_duration == 0.0

    def test_stddev_correct_for_known_values(self):
        # Values: 2, 4, 4, 4, 5, 5, 7, 9
        # Welford uses sample stddev (divides by n-1): sqrt(32/7) ≈ 2.1381
        import math
        w = RouteWindow()
        for v in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]:
            w.update(v, 0.0, 0.0, 1.0, 0.0, 0)
        assert w.stddev_duration == pytest.approx(math.sqrt(32 / 7), abs=1e-9)

    def test_last_duration_tracks_most_recent(self):
        w = RouteWindow()
        w.update(5.0, 0.0, 0.0, 1.0, 0.0, 0)
        w.update(99.0, 0.0, 0.0, 1.0, 0.0, 0)
        assert w.last_duration == 99.0

    def test_last_heavy_ratio_tracks_most_recent(self):
        w = RouteWindow()
        w.update(5.0, 0.1, 0.0, 0.9, 0.0, 0)
        w.update(5.0, 0.9, 0.0, 0.1, 0.0, 0)
        assert w.last_heavy_ratio == pytest.approx(0.9)

    def test_last_ingest_lag_ms_updated(self):
        w = RouteWindow()
        w.update(5.0, 0.0, 0.0, 1.0, 0.0, 500)
        assert w.last_ingest_lag_ms == 500

    def test_max_severe_segments_tracks_maximum(self):
        w = RouteWindow()
        for segs in [1.0, 5.0, 3.0, 2.0]:
            w.update(5.0, 0.0, 0.0, 1.0, segs, 0)
        assert w.max_severe_segments == 5.0


@pytest.mark.unit
class TestRouteWindowProperties:
    def test_mean_heavy_ratio_is_rolling_average(self):
        w = RouteWindow()
        w.update(5.0, 0.2, 0.0, 0.8, 0.0, 0)
        w.update(5.0, 0.4, 0.0, 0.6, 0.0, 0)
        assert w.mean_heavy_ratio == pytest.approx(0.3)

    def test_mean_moderate_ratio_is_rolling_average(self):
        w = RouteWindow()
        w.update(5.0, 0.0, 0.1, 0.9, 0.0, 0)
        w.update(5.0, 0.0, 0.3, 0.7, 0.0, 0)
        assert w.mean_moderate_ratio == pytest.approx(0.2)

    def test_mean_low_ratio_is_rolling_average(self):
        w = RouteWindow()
        w.update(5.0, 0.0, 0.0, 0.6, 0.0, 0)
        w.update(5.0, 0.0, 0.0, 0.4, 0.0, 0)
        assert w.mean_low_ratio == pytest.approx(0.5)

    def test_all_mean_ratios_zero_when_no_updates(self):
        w = RouteWindow()
        assert w.mean_heavy_ratio == 0.0
        assert w.mean_moderate_ratio == 0.0
        assert w.mean_low_ratio == 0.0
        assert w.stddev_duration == 0.0


@pytest.mark.unit
class TestRouteWindowSerialisation:
    def test_to_dict_contains_required_keys(self):
        w = RouteWindow(window_start_ts=1000, count=3, mean_duration=5.0)
        d = w.to_dict()
        for key in ("window_start_ts", "count", "mean_duration", "last_duration"):
            assert key in d

    def test_from_dict_roundtrip(self):
        w = RouteWindow(window_start_ts=999, count=5, mean_duration=12.5, M2_duration=8.0)
        w2 = RouteWindow.from_dict(w.to_dict())
        assert w2.window_start_ts == w.window_start_ts
        assert w2.count == w.count
        assert w2.mean_duration == pytest.approx(w.mean_duration)
