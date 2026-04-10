"""Isolation Forest anomaly detection model wrapper.

Unsupervised model that detects multivariate anomalies by isolating
observations in random trees. Catches patterns that single-variable
Z-score detection misses (e.g., normal duration but abnormal combination
of heavy_ratio + severe_segments).
"""

import logging

import numpy as np
import pyarrow as pa
from sklearn.ensemble import IsolationForest

from ml.features.traffic_features import FEATURE_COLUMNS

logger = logging.getLogger(__name__)

DEFAULT_CONTAMINATION = 0.05
DEFAULT_N_ESTIMATORS = 100
DEFAULT_RANDOM_STATE = 42


class IsolationForestDetector:
    """Scikit-learn IsolationForest wrapper for traffic anomaly detection."""

    def __init__(
        self,
        contamination: float = DEFAULT_CONTAMINATION,
        n_estimators: int = DEFAULT_N_ESTIMATORS,
        random_state: int = DEFAULT_RANDOM_STATE,
    ) -> None:
        self.contamination = contamination
        self.n_estimators = n_estimators
        self.random_state = random_state
        self.model = IsolationForest(
            contamination=contamination,
            n_estimators=n_estimators,
            random_state=random_state,
        )

    def _to_numpy(self, features: pa.Table) -> np.ndarray:
        """Extract feature columns as a 2D numpy array."""
        arrays = [features.column(col).to_numpy().astype(np.float64) for col in FEATURE_COLUMNS]
        X = np.column_stack(arrays)
        # Replace NaN/inf with 0 for robustness
        X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)
        return X  # type: ignore[no-any-return]

    def fit(self, features: pa.Table) -> "IsolationForestDetector":
        """Train the Isolation Forest on feature table."""
        X = self._to_numpy(features)
        self.model.fit(X)
        logger.info("IsolationForest: trained on %d samples, %d features", X.shape[0], X.shape[1])
        return self

    def predict(self, features: pa.Table) -> np.ndarray:
        """Return anomaly labels: 1 = anomaly, 0 = normal.

        sklearn IsolationForest returns -1 for anomalies, 1 for normal.
        We convert to 1 = anomaly, 0 = normal.
        """
        X = self._to_numpy(features)
        raw_labels = self.model.predict(X)
        labels = (raw_labels == -1).astype(np.int32)
        n_anomalies = int(labels.sum())
        logger.info("IsolationForest: %d/%d anomalies", n_anomalies, len(labels))
        return labels  # type: ignore[no-any-return]

    def decision_scores(self, features: pa.Table) -> np.ndarray:
        """Return anomaly scores (lower = more anomalous)."""
        X = self._to_numpy(features)
        return self.model.decision_function(X)  # type: ignore[no-any-return]

    def get_params(self) -> dict[str, object]:
        """Return model parameters for MLflow logging."""
        return {
            "if_contamination": self.contamination,
            "if_n_estimators": self.n_estimators,
            "if_random_state": self.random_state,
        }
