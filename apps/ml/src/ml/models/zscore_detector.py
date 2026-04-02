"""Z-score based statistical anomaly detector.

Simple threshold-based detector: if |duration_zscore| exceeds a configurable
threshold, the observation is flagged as anomalous. Fast, interpretable,
and requires no training — works directly on the feature table.
"""

import logging

import numpy as np
import pyarrow as pa

from ml.features.traffic_features import FEATURE_COLUMNS

logger = logging.getLogger(__name__)

DEFAULT_ZSCORE_THRESHOLD = 3.0


class ZScoreDetector:
    """Threshold-based anomaly detector using pre-computed Z-scores."""

    def __init__(self, threshold: float = DEFAULT_ZSCORE_THRESHOLD) -> None:
        self.threshold = threshold

    def predict(self, features: pa.Table) -> np.ndarray:
        """Return anomaly labels: 1 = anomaly, 0 = normal.

        An observation is anomalous if |duration_zscore| > threshold.
        """
        zscores = features.column("duration_zscore").to_numpy()
        labels = (np.abs(zscores) > self.threshold).astype(np.int32)
        n_anomalies = int(labels.sum())
        logger.info(
            "ZScoreDetector: %d/%d anomalies (threshold=%.1f)",
            n_anomalies, len(labels), self.threshold,
        )
        return labels

    def get_params(self) -> dict[str, float]:
        """Return model parameters for MLflow logging."""
        return {"zscore_threshold": self.threshold}
