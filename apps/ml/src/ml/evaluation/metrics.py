"""Evaluation metrics for anomaly detection model performance.

Since we don't have ground-truth labels (unsupervised), we use
proxy metrics: anomaly rate, agreement between models, and
score distributions.
"""

import logging

import numpy as np

logger = logging.getLogger(__name__)


def compute_metrics(
    zscore_labels: np.ndarray,
    iforest_labels: np.ndarray,
) -> dict[str, float]:
    """Compute evaluation metrics comparing two detectors.

    Returns a dict of metrics suitable for MLflow logging.
    """
    n = len(zscore_labels)

    zscore_anomaly_rate = float(zscore_labels.sum()) / n if n > 0 else 0.0
    iforest_anomaly_rate = float(iforest_labels.sum()) / n if n > 0 else 0.0

    # Agreement: fraction of samples where both models agree
    agreement = float((zscore_labels == iforest_labels).sum()) / n if n > 0 else 0.0

    # Both flagged as anomaly
    both_anomaly = float(((zscore_labels == 1) & (iforest_labels == 1)).sum())
    # Either flagged as anomaly
    either_anomaly = float(((zscore_labels == 1) | (iforest_labels == 1)).sum())

    # Jaccard similarity of anomaly sets
    jaccard = both_anomaly / either_anomaly if either_anomaly > 0 else 0.0

    metrics = {
        "total_samples": float(n),
        "zscore_anomaly_rate": zscore_anomaly_rate,
        "iforest_anomaly_rate": iforest_anomaly_rate,
        "model_agreement": agreement,
        "anomaly_jaccard": jaccard,
        "both_anomaly_count": both_anomaly,
        "either_anomaly_count": either_anomaly,
    }

    logger.info(
        "Metrics: samples=%d, zscore_rate=%.3f, iforest_rate=%.3f, agreement=%.3f",
        n, zscore_anomaly_rate, iforest_anomaly_rate, agreement,
    )
    return metrics
