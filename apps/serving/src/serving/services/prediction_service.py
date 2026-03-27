"""Prediction service: loads the latest IsolationForest model from MLflow
and scores current online features fetched from Postgres.

Model is cached in-process and refreshed every MODEL_TTL seconds so that
new training runs are automatically picked up without a restart.

Feature mapping from online_route_features → IsolationForest input:
    duration_zscore       → duration_zscore         (direct)
    heavy_ratio_deviation → mean_heavy_ratio         (proxy; no per-route baseline in serving)
    p95_to_mean_ratio     → last_duration / mean     (proxy; p95 not stored online)
    observation_count     → observation_count        (direct)
    max_severe_segments   → 0.0                      (not collected online; neutral fill)
"""

import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)

# How often to re-load the model from MLflow (seconds)
MODEL_TTL = 3600.0

# Registered model name — must match what ml/train.py uses
_REGISTERED_MODEL = "traffic-anomaly-iforest"
_MODEL_ALIAS = "champion"   # fall back to latest version if alias not set

# Feature order MUST match ml/features/traffic_features.py FEATURE_COLUMNS
_FEATURE_ORDER = [
    "duration_zscore",
    "heavy_ratio_deviation",
    "p95_to_mean_ratio",
    "observation_count",
    "max_severe_segments",
]


@dataclass
class PredictionResult:
    route_id: str
    iforest_score: float        # raw decision score (lower = more anomalous)
    iforest_anomaly: bool       # True when model flags as anomaly
    zscore_anomaly: bool        # from online consumer (stored in Postgres)
    both_anomaly: bool          # both methods agree


@dataclass
class _ModelCache:
    model: Any = None
    loaded_at: float = 0.0
    model_uri: str = ""
    last_error: str | None = None


_cache = _ModelCache()


def _mlflow_tracking_uri() -> str:
    return os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")


def _load_model() -> Any:
    """Load the latest registered IsolationForest from MLflow.

    Uses mlflow.artifacts.download_artifacts + joblib to avoid the implicit
    pandas dependency that mlflow.sklearn.load_model pulls in.
    """
    import tempfile

    import joblib
    import mlflow
    from mlflow.tracking import MlflowClient

    mlflow.set_tracking_uri(_mlflow_tracking_uri())
    client = MlflowClient()

    # Resolve the model version: try @champion alias, fall back to latest
    try:
        mv = client.get_model_version_by_alias(_REGISTERED_MODEL, _MODEL_ALIAS)
        version = mv.version
        logger.info("prediction_service: resolved %s@%s → version %s", _REGISTERED_MODEL, _MODEL_ALIAS, version)
    except Exception:
        versions = client.get_latest_versions(_REGISTERED_MODEL)
        if not versions:
            raise RuntimeError(f"No versions found for model '{_REGISTERED_MODEL}'")
        version = versions[-1].version
        logger.info("prediction_service: alias not set, using latest version %s", version)

    mv = client.get_model_version(_REGISTERED_MODEL, version)
    artifact_uri = mv.source  # e.g. s3://mlflow/.../artifacts/isolation_forest

    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = mlflow.artifacts.download_artifacts(artifact_uri=artifact_uri, dst_path=tmpdir)
        # artifact is a directory; the pickle is model.pkl inside it
        pkl_path = os.path.join(local_path, "model.pkl")
        model = joblib.load(pkl_path)

    logger.info("prediction_service: loaded IsolationForest version %s", version)
    return model, artifact_uri


def _get_model() -> Any:
    """Return cached model, refreshing if TTL has elapsed."""
    now = time.monotonic()
    if _cache.model is None or (now - _cache.loaded_at) > MODEL_TTL:
        try:
            model, uri = _load_model()
            _cache.model = model
            _cache.loaded_at = now
            _cache.model_uri = uri
            _cache.last_error = None
        except Exception as exc:
            _cache.last_error = str(exc)
            logger.error("prediction_service: failed to load model: %s", exc)
            raise
    return _cache.model


def _build_feature_matrix(rows: list[dict[str, Any]]) -> tuple[np.ndarray, list[str]]:
    """Convert Postgres rows → numpy matrix in FEATURE_ORDER.

    Returns (X, route_ids) where X[i] corresponds to route_ids[i].
    """
    route_ids: list[str] = []
    vectors: list[list[float]] = []

    for row in rows:
        mean_dur = row.get("mean_duration_minutes") or 0.0
        last_dur = row.get("last_duration_minutes") or mean_dur

        duration_zscore      = float(row.get("duration_zscore") or 0.0)
        heavy_ratio_deviation = float(row.get("mean_heavy_ratio") or 0.0)
        p95_to_mean_ratio    = (last_dur / mean_dur) if mean_dur > 0 else 1.0
        observation_count    = float(row.get("observation_count") or 0)
        max_severe_segments  = 0.0   # not collected in online path

        route_ids.append(row["route_id"])
        vectors.append([
            duration_zscore,
            heavy_ratio_deviation,
            p95_to_mean_ratio,
            observation_count,
            max_severe_segments,
        ])

    X = np.array(vectors, dtype=np.float64)
    X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)
    return X, route_ids


def score_rows(rows: list[dict[str, Any]]) -> list[PredictionResult]:
    """Score a list of online feature rows with the IsolationForest model.

    Args:
        rows: dicts from asyncpg fetchall (online_route_features columns).

    Returns:
        List of PredictionResult, one per row, in the same order.
    """
    if not rows:
        return []

    model = _get_model()
    X, route_ids = _build_feature_matrix(rows)

    # sklearn IsolationForest: predict() returns -1 (anomaly) or +1 (normal)
    raw_labels: np.ndarray = model.predict(X)
    scores: np.ndarray = model.decision_function(X)

    results: list[PredictionResult] = []
    for i, route_id in enumerate(route_ids):
        iforest_anomaly = bool(raw_labels[i] == -1)
        zscore_anomaly  = bool(rows[i].get("is_anomaly", False))
        results.append(PredictionResult(
            route_id=route_id,
            iforest_score=float(scores[i]),
            iforest_anomaly=iforest_anomaly,
            zscore_anomaly=zscore_anomaly,
            both_anomaly=iforest_anomaly and zscore_anomaly,
        ))

    n_if = sum(1 for r in results if r.iforest_anomaly)
    n_both = sum(1 for r in results if r.both_anomaly)
    logger.info(
        "prediction_service: scored %d routes — iforest=%d anomalies, both=%d",
        len(results), n_if, n_both,
    )
    return results
