"""Prediction service: loads per-route IsolationForest models from MLflow
and scores current online features fetched from Postgres.

Each route has its own registered model: "iforest-{route_id}".
Models are cached in-process per route and refreshed every MODEL_TTL seconds.

Feature mapping from online_route_features → IsolationForest input:
    mean_heavy_ratio    → avg_heavy_ratio    (raw congestion signal)
    mean_moderate_ratio → avg_moderate_ratio (raw congestion signal)
    mean_low_ratio      → avg_low_ratio      (raw congestion signal)
    max_severe_segments → max_severe_segments
    hour_of_day         → derived from window_start
    day_of_week         → derived from window_start
"""

import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)

MODEL_TTL = 3600.0  # seconds before reloading from MLflow

# Registered model name prefix — must match ml/train.py
_MODEL_PREFIX = "iforest-"
_MODEL_ALIAS = "champion"

# Feature order MUST match ml/features/traffic_features.py FEATURE_COLUMNS
_FEATURE_ORDER = [
    "avg_heavy_ratio",
    "avg_moderate_ratio",
    "avg_low_ratio",
    "max_severe_segments",
    "hour_of_day",
    "day_of_week",
]


@dataclass
class PredictionResult:
    route_id: str
    iforest_score: float
    iforest_anomaly: bool
    zscore_anomaly: bool
    both_anomaly: bool


@dataclass
class _RouteModelCache:
    model: Any = None
    loaded_at: float = 0.0
    model_uri: str = ""
    last_error: str | None = None


# Per-route cache: route_id → _RouteModelCache
_cache: dict[str, _RouteModelCache] = {}
# Track overall last load for /predict/model/info
_last_load_at: float = 0.0
_last_uri: str = ""


def _mlflow_tracking_uri() -> str:
    return os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")


def _load_route_model(route_id: str) -> tuple[Any, str]:
    """Load the latest IsolationForest for a specific route from MLflow."""
    import tempfile

    import joblib
    import mlflow
    from mlflow.tracking import MlflowClient

    mlflow.set_tracking_uri(_mlflow_tracking_uri())
    client = MlflowClient()
    registered_name = f"{_MODEL_PREFIX}{route_id}"

    try:
        mv = client.get_model_version_by_alias(registered_name, _MODEL_ALIAS)
        version = mv.version
        logger.info("prediction_service: %s@%s → version %s", registered_name, _MODEL_ALIAS, version)
    except Exception:
        versions = client.get_latest_versions(registered_name)
        if not versions:
            raise RuntimeError(f"No model found for route '{route_id}' (registered: {registered_name})")
        version = versions[-1].version
        logger.info("prediction_service: %s alias not set, using latest version %s", registered_name, version)

    mv = client.get_model_version(registered_name, version)
    artifact_uri = mv.source

    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = mlflow.artifacts.download_artifacts(artifact_uri=artifact_uri, dst_path=tmpdir)
        pkl_path = os.path.join(local_path, "model.pkl")
        model = joblib.load(pkl_path)

    return model, artifact_uri


def _get_route_model(route_id: str) -> Any:
    """Return cached model for route, refreshing if TTL elapsed or not loaded."""
    global _last_load_at, _last_uri

    now = time.monotonic()
    entry = _cache.get(route_id)

    if entry is None or entry.model is None or (now - entry.loaded_at) > MODEL_TTL:
        if entry is None:
            entry = _RouteModelCache()
            _cache[route_id] = entry
        try:
            model, uri = _load_route_model(route_id)
            entry.model = model
            entry.loaded_at = now
            entry.model_uri = uri
            entry.last_error = None
            _last_load_at = now
            _last_uri = uri
        except Exception as exc:
            entry.last_error = str(exc)
            logger.warning("prediction_service: no model for route=%s — %s", route_id, exc)
            raise

    return entry.model


def _build_feature_vector(row: dict[str, Any]) -> np.ndarray:
    """Convert one Postgres row → 1D feature vector.

    Congestion ratios are tracked by the online service and stored in Postgres.
    hour_of_day and day_of_week are derived from window_start (UTC).
    Feature order MUST match FEATURE_COLUMNS in ml/features/traffic_features.py.
    """
    from datetime import timezone
    window_start = row.get("window_start")
    if window_start is not None and hasattr(window_start, "astimezone"):
        window_start = window_start.astimezone(timezone.utc)
        hour_of_day = float(window_start.hour)
        day_of_week = float(window_start.weekday() + 1) % 7  # match SQL EXTRACT(DOW)
    else:
        hour_of_day = 0.0
        day_of_week = 0.0

    return np.array([
        float(row.get("mean_heavy_ratio") or 0.0),
        float(row.get("mean_moderate_ratio") or 0.0),
        float(row.get("mean_low_ratio") or 0.0),
        float(row.get("max_severe_segments") or 0.0),
        hour_of_day,
        day_of_week,
    ], dtype=np.float64)


def score_rows(rows: list[dict[str, Any]]) -> list[PredictionResult]:
    """Score each row with its route-specific IsolationForest model.

    Routes with no trained model are scored as non-anomalous (safe default).
    """
    if not rows:
        return []

    results: list[PredictionResult] = []
    for row in rows:
        route_id = row["route_id"]
        zscore_anomaly = bool(row.get("is_anomaly", False))

        try:
            model = _get_route_model(route_id)
            X = np.nan_to_num(
                _build_feature_vector(row).reshape(1, -1),
                nan=0.0, posinf=0.0, neginf=0.0,
            )
            iforest_anomaly = bool(model.predict(X)[0] == -1)
            iforest_score = float(model.decision_function(X)[0])
        except Exception:
            # No model for this route yet — fall back to zscore only
            iforest_anomaly = False
            iforest_score = 0.0

        results.append(PredictionResult(
            route_id=route_id,
            iforest_score=iforest_score,
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


def cache_info() -> dict[str, Any]:
    """Return aggregate cache status for /predict/model/info."""
    now = time.monotonic()
    loaded_routes = [rid for rid, e in _cache.items() if e.model is not None]
    oldest_load = min((e.loaded_at for e in _cache.values() if e.model is not None), default=0.0)
    age = (now - oldest_load) if oldest_load else None
    return {
        "loaded_routes": len(loaded_routes),
        "total_routes_seen": len(_cache),
        "age_seconds": round(age, 1) if age is not None else None,
        "ttl_seconds": MODEL_TTL,
        "next_refresh_seconds": round(max(0.0, MODEL_TTL - age), 1) if age is not None else None,
        "model_uri": _last_uri or None,
    }


def route_cache_status() -> list[dict[str, Any]]:
    """Return per-route model cache status for /predict/model/routes."""
    now = time.monotonic()
    rows: list[dict[str, Any]] = []
    for route_id, entry in sorted(_cache.items()):
        loaded = entry.model is not None
        age = round(now - entry.loaded_at, 1) if loaded else None
        rows.append({
            "route_id": route_id,
            "loaded": loaded,
            "age_seconds": age,
            "next_refresh_seconds": round(max(0.0, MODEL_TTL - age), 1) if age is not None else None,
            "model_uri": entry.model_uri or None,
            "error": entry.last_error,
        })
    return rows
