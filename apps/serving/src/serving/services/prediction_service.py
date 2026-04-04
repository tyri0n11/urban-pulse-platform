"""Prediction service: loads per-route IsolationForest models from MLflow
and scores current online features fetched from Postgres.

Each route has its own registered model: "iforest-{route_id}".
One artifact is loaded from the isolation_forest/ path in MLflow:
  model.pkl      — plain sklearn IsolationForest

At scoring time the serving layer reconstructs the same cyclical time-encoded
features that the model was trained on (ml/features/traffic_features.py):
  avg_heavy_ratio      — from online_route_features.mean_heavy_ratio
  avg_moderate_ratio   — from online_route_features.mean_moderate_ratio
  max_severe_segments  — from online_route_features.max_severe_segments
  hour_sin / hour_cos  — cyclical encoding of window_start hour (period 24)
  dow_sin  / dow_cos   — cyclical encoding of window_start day-of-week (period 7)
"""

import logging
import os
import time
from dataclasses import dataclass
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
    "max_severe_segments",
    "hour_sin",
    "hour_cos",
    "dow_sin",
    "dow_cos",
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
    """Load the IsolationForest model for a specific route from MLflow.

    Returns (model, artifact_uri).
    """
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


def _get_route_cache(route_id: str) -> _RouteModelCache:
    """Return populated cache entry for route, refreshing if TTL elapsed or not loaded."""
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

    return entry


def _build_feature_vector(row: dict[str, Any]) -> np.ndarray:
    """Convert one Postgres row → cyclical time-encoded feature vector.

    Computes the same features as ml/features/traffic_features.py FEATURE_COLUMNS:
      avg_heavy_ratio, avg_moderate_ratio, max_severe_segments,
      hour_sin, hour_cos, dow_sin, dow_cos

    window_start must be present for accurate time encoding; defaults to
    Sunday midnight (0, 0) if missing.
    """
    import math
    from datetime import timezone

    window_start = row.get("window_start")
    if window_start is not None and hasattr(window_start, "astimezone"):
        window_start = window_start.astimezone(timezone.utc)
        hour = window_start.hour
        dow = (window_start.weekday() + 1) % 7  # SQL DOW: Sun=0…Sat=6
    else:
        hour = 0
        dow = 0

    heavy = float(row.get("mean_heavy_ratio") or 0.0)
    moderate = float(row.get("mean_moderate_ratio") or 0.0)
    max_severe = float(row.get("max_severe_segments") or 0.0)

    hour_sin = math.sin(2 * math.pi * hour / 24)
    hour_cos = math.cos(2 * math.pi * hour / 24)
    dow_sin  = math.sin(2 * math.pi * dow  / 7)
    dow_cos  = math.cos(2 * math.pi * dow  / 7)

    return np.array([
        heavy,
        moderate,
        max_severe,
        hour_sin,
        hour_cos,
        dow_sin,
        dow_cos,
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
            cache_entry = _get_route_cache(route_id)
            X = np.nan_to_num(
                _build_feature_vector(row).reshape(1, -1),
                nan=0.0, posinf=0.0, neginf=0.0,
            )
            iforest_anomaly = bool(cache_entry.model.predict(X)[0] == -1)
            iforest_score = float(cache_entry.model.decision_function(X)[0])
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
