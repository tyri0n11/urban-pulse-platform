"""Prediction service: loads per-route IsolationForest models from MLflow
and scores current online features fetched from Postgres.

Each route has its own registered model: "iforest-{route_id}".
Two artifacts are loaded from the isolation_forest/ path in MLflow:
  model.pkl      — plain sklearn IsolationForest
  baseline.json  — per-(dow, hour) baseline stats (produced by ml/train.py)

At scoring time the serving layer reconstructs the same deviation-based
features that the model was trained on:
  heavy_ratio_deviation    = mean_heavy_ratio - baseline_heavy_mean
  moderate_ratio_deviation = mean_moderate_ratio - baseline_mod_mean
  heavy_ratio_zscore       = deviation / baseline_heavy_stddev
  moderate_ratio_zscore    = deviation / baseline_mod_stddev
  max_severe_segments      (absolute)

Baseline key format: "{day_of_week}_{hour_of_day}" (SQL DOW: Sun=0…Sat=6).
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
    "heavy_ratio_deviation",
    "moderate_ratio_deviation",
    "heavy_ratio_zscore",
    "moderate_ratio_zscore",
    "max_severe_segments",
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
    baseline: dict[str, Any] = field(default_factory=dict)
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


def _load_route_model(route_id: str) -> tuple[Any, dict[str, Any], str]:
    """Load the IsolationForest model and baseline.json for a specific route from MLflow.

    Returns (model, baseline_dict, artifact_uri).
    baseline_dict keys are "{day_of_week}_{hour_of_day}" strings.
    """
    import json
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

        # Load baseline.json saved alongside the model
        baseline_json_path = os.path.join(local_path, "baseline.json")
        if os.path.exists(baseline_json_path):
            with open(baseline_json_path) as f:
                baseline: dict[str, Any] = json.load(f)
        else:
            logger.warning("prediction_service: baseline.json not found for route=%s — deviation features will default to 0", route_id)
            baseline = {}

    return model, baseline, artifact_uri


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
            model, baseline, uri = _load_route_model(route_id)
            entry.model = model
            entry.baseline = baseline
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


def _build_feature_vector(row: dict[str, Any], baseline: dict[str, Any]) -> np.ndarray:
    """Convert one Postgres row → deviation-based feature vector.

    Computes the same features as ml/features/traffic_features.py FEATURE_COLUMNS:
      heavy_ratio_deviation, moderate_ratio_deviation,
      heavy_ratio_zscore, moderate_ratio_zscore, max_severe_segments

    baseline key format: "{day_of_week}_{hour_of_day}" (SQL DOW: Sun=0…Sat=6).
    Falls back to zero-deviation if no baseline slot is found.
    """
    from datetime import timezone

    window_start = row.get("window_start")
    if window_start is not None and hasattr(window_start, "astimezone"):
        window_start = window_start.astimezone(timezone.utc)
        hour_of_day = window_start.hour
        day_of_week = (window_start.weekday() + 1) % 7  # SQL DOW: Sun=0…Sat=6
    else:
        hour_of_day = 0
        day_of_week = 0

    slot_key = f"{day_of_week}_{hour_of_day}"
    slot = baseline.get(slot_key, {})

    heavy = float(row.get("mean_heavy_ratio") or 0.0)
    moderate = float(row.get("mean_moderate_ratio") or 0.0)
    max_severe = float(row.get("max_severe_segments") or 0.0)

    heavy_mean   = float(slot.get("heavy_mean", heavy))
    heavy_stddev = float(slot.get("heavy_stddev", 1.0)) or 1.0
    mod_mean     = float(slot.get("mod_mean", moderate))
    mod_stddev   = float(slot.get("mod_stddev", 1.0)) or 1.0

    heavy_dev    = heavy - heavy_mean
    mod_dev      = moderate - mod_mean
    heavy_zscore = heavy_dev / heavy_stddev
    mod_zscore   = mod_dev / mod_stddev

    return np.array([
        heavy_dev,
        mod_dev,
        heavy_zscore,
        mod_zscore,
        max_severe,
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
                _build_feature_vector(row, cache_entry.baseline).reshape(1, -1),
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
