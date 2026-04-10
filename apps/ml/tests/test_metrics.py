"""Tests for evaluation.compute_metrics."""
import numpy as np
import pytest

from ml.evaluation.metrics import compute_metrics


@pytest.mark.unit
class TestComputeMetrics:
    def test_all_normal_both_detectors(self):
        z = np.zeros(10, dtype=np.int32)
        i = np.zeros(10, dtype=np.int32)
        m = compute_metrics(z, i)
        assert m["zscore_anomaly_rate"] == 0.0
        assert m["iforest_anomaly_rate"] == 0.0
        assert m["model_agreement"] == 1.0
        assert m["anomaly_jaccard"] == 0.0

    def test_all_anomaly_both_detectors(self):
        z = np.ones(8, dtype=np.int32)
        i = np.ones(8, dtype=np.int32)
        m = compute_metrics(z, i)
        assert m["zscore_anomaly_rate"] == 1.0
        assert m["iforest_anomaly_rate"] == 1.0
        assert m["model_agreement"] == 1.0
        assert m["anomaly_jaccard"] == pytest.approx(1.0)

    def test_perfect_disagreement(self):
        # zscore says all normal; iforest says all anomaly
        z = np.zeros(4, dtype=np.int32)
        i = np.ones(4, dtype=np.int32)
        m = compute_metrics(z, i)
        assert m["model_agreement"] == 0.0
        assert m["both_anomaly_count"] == 0.0
        assert m["anomaly_jaccard"] == 0.0

    def test_mixed_partial_agreement(self):
        z = np.array([1, 1, 0, 0], dtype=np.int32)
        i = np.array([1, 0, 1, 0], dtype=np.int32)
        m = compute_metrics(z, i)
        # agree on index 0 (both=1) and index 3 (both=0) → agreement = 2/4 = 0.5
        assert m["model_agreement"] == pytest.approx(0.5)
        # both=1 only at index 0; either=1 at 0,1,2 → jaccard = 1/3
        assert m["anomaly_jaccard"] == pytest.approx(1 / 3)

    def test_total_samples_correct(self):
        z = np.zeros(7, dtype=np.int32)
        i = np.zeros(7, dtype=np.int32)
        m = compute_metrics(z, i)
        assert m["total_samples"] == 7.0

    def test_empty_arrays_returns_zeros(self):
        m = compute_metrics(np.array([]), np.array([]))
        assert m["model_agreement"] == 0.0
        assert m["zscore_anomaly_rate"] == 0.0

    def test_either_anomaly_count_is_union(self):
        z = np.array([1, 1, 0, 0], dtype=np.int32)
        i = np.array([0, 1, 1, 0], dtype=np.int32)
        m = compute_metrics(z, i)
        # union: indices 0,1,2 → 3
        assert m["either_anomaly_count"] == 3.0
        assert m["both_anomaly_count"] == 1.0  # only index 1
