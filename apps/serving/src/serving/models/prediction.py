"""Domain model for ML prediction results."""

from dataclasses import dataclass


@dataclass
class PredictionResult:
    route_id: str
    iforest_score: float
    iforest_anomaly: bool
    zscore_anomaly: bool
    both_anomaly: bool
