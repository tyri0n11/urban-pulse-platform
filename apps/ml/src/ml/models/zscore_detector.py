"""Heavy-ratio based statistical anomaly detector.

Simple threshold-based detector: if avg_heavy_ratio exceeds a configurable
threshold, the observation is flagged as anomalous. Used as a comparison
baseline against the IsolationForest model.
"""

import logging

import numpy as np
import pyarrow as pa

logger = logging.getLogger(__name__)

DEFAULT_HEAVY_THRESHOLD = 0.3  # >30% heavy traffic = anomalous


class ZScoreDetector:
    """Threshold-based anomaly detector using heavy congestion ratio."""

    def __init__(self, threshold: float = DEFAULT_HEAVY_THRESHOLD) -> None:
        self.threshold = threshold

    def predict(self, features: pa.Table) -> np.ndarray:
        """Return anomaly labels: 1 = anomaly, 0 = normal.

        An observation is anomalous if avg_heavy_ratio > threshold.
        """
        heavy = features.column("avg_heavy_ratio").to_numpy()
        labels = (heavy > self.threshold).astype(np.int32)
        n_anomalies = int(labels.sum())
        logger.info(
            "HeavyRatioDetector: %d/%d anomalies (threshold=%.2f)",
            n_anomalies, len(labels), self.threshold,
        )
        return labels  # type: ignore[no-any-return]

    def get_params(self) -> dict[str, float]:
        """Return model parameters for MLflow logging."""
        return {"heavy_threshold": self.threshold}
