"""Tests for IsolationForestDetector and ZScoreDetector."""
import numpy as np
import pyarrow as pa
import pytest

from ml.models.isolation_forest import IsolationForestDetector
from ml.models.zscore_detector import ZScoreDetector
from conftest import make_feature_table


# ── IsolationForestDetector ───────────────────────────────────────────────────

@pytest.mark.unit
class TestIsolationForestDetector:
    def test_fit_returns_self(self, feature_table):
        det = IsolationForestDetector(n_estimators=10)
        result = det.fit(feature_table)
        assert result is det

    def test_predict_returns_binary_labels(self, feature_table):
        det = IsolationForestDetector(n_estimators=10, random_state=0)
        det.fit(feature_table)
        labels = det.predict(feature_table)
        assert set(np.unique(labels)).issubset({0, 1})

    def test_predict_label_count_matches_rows(self, feature_table):
        det = IsolationForestDetector(n_estimators=10, random_state=0)
        det.fit(feature_table)
        labels = det.predict(feature_table)
        assert len(labels) == feature_table.num_rows

    def test_predict_no_minus_one_labels(self, feature_table):
        """sklearn returns -1 for anomalies; wrapper must convert to 1."""
        det = IsolationForestDetector(n_estimators=10, random_state=0)
        det.fit(feature_table)
        labels = det.predict(feature_table)
        assert -1 not in labels

    def test_decision_scores_length_matches_rows(self, feature_table):
        det = IsolationForestDetector(n_estimators=10, random_state=0)
        det.fit(feature_table)
        scores = det.decision_scores(feature_table)
        assert len(scores) == feature_table.num_rows

    def test_contamination_affects_anomaly_rate(self, feature_table):
        """Higher contamination → more anomalies predicted."""
        det_low = IsolationForestDetector(contamination=0.02, n_estimators=10, random_state=0)
        det_high = IsolationForestDetector(contamination=0.3, n_estimators=10, random_state=0)
        det_low.fit(feature_table)
        det_high.fit(feature_table)
        rate_low = det_low.predict(feature_table).mean()
        rate_high = det_high.predict(feature_table).mean()
        assert rate_high >= rate_low

    def test_handles_nan_in_features(self):
        """NaN values in feature table should not cause errors."""
        table = make_feature_table(n_rows=20)
        # Inject NaN into avg_heavy_ratio
        heavy = table.column("avg_heavy_ratio").to_pylist()
        heavy[0] = float("nan")
        table = table.set_column(
            table.schema.get_field_index("avg_heavy_ratio"),
            "avg_heavy_ratio",
            pa.array(heavy),
        )
        det = IsolationForestDetector(n_estimators=10, random_state=0)
        det.fit(table)
        labels = det.predict(table)
        assert len(labels) == 20

    def test_get_params_has_expected_keys(self):
        det = IsolationForestDetector(contamination=0.1, n_estimators=50)
        params = det.get_params()
        assert "if_contamination" in params
        assert "if_n_estimators" in params
        assert "if_random_state" in params
        assert params["if_contamination"] == 0.1
        assert params["if_n_estimators"] == 50


# ── ZScoreDetector ────────────────────────────────────────────────────────────

@pytest.mark.unit
class TestZScoreDetector:
    def _table(self, heavy_ratios: list[float]) -> pa.Table:
        return pa.table({"avg_heavy_ratio": heavy_ratios})

    def test_all_below_threshold_returns_zeros(self):
        det = ZScoreDetector(threshold=0.3)
        labels = det.predict(self._table([0.1, 0.2, 0.25]))
        assert list(labels) == [0, 0, 0]

    def test_all_above_threshold_returns_ones(self):
        det = ZScoreDetector(threshold=0.3)
        labels = det.predict(self._table([0.4, 0.5, 0.9]))
        assert list(labels) == [1, 1, 1]

    def test_mixed_values(self):
        det = ZScoreDetector(threshold=0.3)
        labels = det.predict(self._table([0.1, 0.35, 0.2, 0.6]))
        assert list(labels) == [0, 1, 0, 1]

    def test_exactly_at_threshold_is_not_anomaly(self):
        det = ZScoreDetector(threshold=0.3)
        labels = det.predict(self._table([0.3]))
        assert labels[0] == 0  # > threshold, not >=

    def test_default_threshold_is_30_pct(self):
        det = ZScoreDetector()
        assert det.threshold == pytest.approx(0.3)

    def test_get_params_returns_threshold(self):
        det = ZScoreDetector(threshold=0.25)
        assert det.get_params()["heavy_threshold"] == pytest.approx(0.25)

    def test_label_count_matches_input_rows(self):
        det = ZScoreDetector()
        labels = det.predict(self._table([0.1] * 10))
        assert len(labels) == 10
