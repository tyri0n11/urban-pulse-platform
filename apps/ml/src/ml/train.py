"""Entry point for model training and MLflow experiment tracking.

Reads gold Iceberg table, engineers cyclical time-encoded features,
trains one IsolationForest per route, and logs everything to MLflow.

Each route run saves one artifact under isolation_forest/:
  model.pkl      — serialised IsolationForestDetector (plain, no sklearn Pipeline)

No baseline.json is saved — the serving layer reconstructs the same cyclical
features (hour_sin/cos, dow_sin/cos) directly from window_start timestamps.
"""

import json
import logging
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import mlflow
import numpy as np
import pyarrow as pa
from mlflow.models import infer_signature

from urbanpulse_core.config import settings
from urbanpulse_infra.iceberg import get_iceberg_catalog
from urbanpulse_infra.mlflow import configure_mlflow, get_or_create_experiment

from ml.evaluation.metrics import compute_metrics
from ml.features.traffic_features import FEATURE_COLUMNS, build_features, list_route_ids
from ml.models.isolation_forest import IsolationForestDetector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

_EXPERIMENT_NAME = "traffic-anomaly-detection"

# Enable sklearn autologging — automatically captures model params, metrics,
# and serialized model. We disable model logging here because we do it manually
# with a custom signature and registered model name.
# log_datasets=False avoids the SQLAlchemy ambiguous-FROM bug in MLflow <2.22
# that breaks the MLflow UI when datasets are logged across multiple runs.
mlflow.sklearn.autolog(log_models=False, log_datasets=False, silent=True)


def _log_feature_stats(features: pa.Table) -> None:
    """Log per-feature distribution statistics as metrics and artifact."""
    stats: dict[str, dict[str, float]] = {}
    for col in FEATURE_COLUMNS:
        values = features.column(col).to_numpy().astype(np.float64)
        values = values[np.isfinite(values)]
        if len(values) == 0:
            continue
        col_stats = {
            "mean": float(np.mean(values)),
            "std": float(np.std(values)),
            "min": float(np.min(values)),
            "max": float(np.max(values)),
            "median": float(np.median(values)),
            "p5": float(np.percentile(values, 5)),
            "p25": float(np.percentile(values, 25)),
            "p75": float(np.percentile(values, 75)),
            "p95": float(np.percentile(values, 95)),
        }
        stats[col] = col_stats
        # Log summary metrics for MLflow UI charts
        mlflow.log_metrics({
            f"feat_{col}_mean": col_stats["mean"],
            f"feat_{col}_std": col_stats["std"],
        })

    # Log full stats as JSON artifact for detailed inspection
    with tempfile.TemporaryDirectory() as tmpdir:
        stats_path = Path(tmpdir) / "feature_stats.json"
        stats_path.write_text(json.dumps(stats, indent=2))
        mlflow.log_artifact(str(stats_path), artifact_path="diagnostics")


def _log_anomaly_details(
    features: pa.Table,
    zscore_labels: np.ndarray,
    iforest_labels: np.ndarray,
) -> None:
    """Log anomaly breakdown per route as a JSON artifact."""
    route_ids = features.column("route_id").to_pylist()
    hours = [str(h) for h in features.column("hour_utc").to_pylist()]

    # Build per-route anomaly summary
    route_summary: dict[str, dict[str, int]] = {}
    for i, route_id in enumerate(route_ids):
        if route_id not in route_summary:
            route_summary[route_id] = {"total": 0, "zscore": 0, "iforest": 0, "both": 0}
        route_summary[route_id]["total"] += 1
        if zscore_labels[i] == 1:
            route_summary[route_id]["zscore"] += 1
        if iforest_labels[i] == 1:
            route_summary[route_id]["iforest"] += 1
        if zscore_labels[i] == 1 and iforest_labels[i] == 1:
            route_summary[route_id]["both"] += 1

    # Collect sample anomalous observations (both models agree)
    both_mask = (zscore_labels == 1) & (iforest_labels == 1)
    sample_anomalies = []
    for i in np.where(both_mask)[0][:20]:  # first 20
        sample_anomalies.append({
            "route_id": route_ids[i],
            "hour_utc": hours[i],
            "features": {col: float(features.column(col)[i].as_py()) for col in FEATURE_COLUMNS},
        })

    with tempfile.TemporaryDirectory() as tmpdir:
        # Route breakdown
        route_path = Path(tmpdir) / "anomaly_by_route.json"
        route_path.write_text(json.dumps(route_summary, indent=2))
        mlflow.log_artifact(str(route_path), artifact_path="diagnostics")

        # Sample anomalies
        if sample_anomalies:
            samples_path = Path(tmpdir) / "sample_anomalies.json"
            samples_path.write_text(json.dumps(sample_anomalies, indent=2))
            mlflow.log_artifact(str(samples_path), artifact_path="diagnostics")


class TrainResult:
    """Result of a training run."""

    def __init__(
        self, run_id: str, status: str, sample_count: int, metrics: dict[str, float]
    ) -> None:
        self.run_id = run_id
        self.status = status
        self.sample_count = sample_count
        self.metrics = metrics


def _train_single_route(
    route_id: str,
    catalog: object,
    parent_run_id: str,
) -> dict[str, object]:
    """Train one IsolationForest per route on cyclical time-encoded features.

    Features (FEATURE_COLUMNS): avg_heavy_ratio, avg_moderate_ratio,
    max_severe_segments, hour_sin, hour_cos, dow_sin, dow_cos.
    No baseline table required — the cyclical encoding embeds time context
    directly so IsolationForest learns joint (ratio × time-of-day) distribution.
    """
    features = build_features(catalog, route_id=route_id)  # type: ignore[arg-type]
    if features is None or features.num_rows < 10:
        logger.warning("route=%s: not enough data (%d rows) — skipping", route_id, features.num_rows if features else 0)
        return {"route_id": route_id, "status": "skipped", "sample_count": 0}

    registered_name = f"iforest-{route_id}"

    with mlflow.start_run(
        run_name=f"route-{route_id}",
        parent_run_id=parent_run_id,
        tags={"route_id": route_id, "pipeline": "per-route-iforest"},
        nested=True,
    ):
        mlflow.log_params({
            "route_id": route_id,
            "sample_count": features.num_rows,
            "feature_columns": FEATURE_COLUMNS,
        })

        # Extract cyclical feature matrix
        arrays = [features.column(col).to_numpy().astype(np.float64) for col in FEATURE_COLUMNS]
        X = np.nan_to_num(np.column_stack(arrays), nan=0.0, posinf=0.0, neginf=0.0)

        # Fit IsolationForest on cyclical time-encoded features
        iforest_params = {"contamination": 0.05, "n_estimators": 100, "random_state": 42}
        detector = IsolationForestDetector(**iforest_params)  # type: ignore[arg-type]
        mlflow.log_params({f"if_{k}": v for k, v in iforest_params.items()})
        detector.model.fit(X)

        raw_labels = detector.model.predict(X)
        iforest_labels = (raw_labels == -1).astype(np.int32)
        scores = detector.model.decision_function(X)

        mlflow.log_metrics({
            "iforest_anomaly_count": int(iforest_labels.sum()),
            "iforest_anomaly_rate": float(iforest_labels.sum()) / len(iforest_labels),
            "iforest_score_mean": float(np.mean(scores)),
            "iforest_score_p95": float(np.percentile(scores, 95)),
        })

        # Z-score labels for cross-model comparison: simple route-level z-score
        # on avg_heavy_ratio (route mean ± 2σ threshold)
        heavy_vals = features.column("avg_heavy_ratio").to_numpy().astype(np.float64)
        heavy_mean = float(np.nanmean(heavy_vals))
        heavy_std = float(np.nanstd(heavy_vals)) or 1.0
        heavy_zscores = (heavy_vals - heavy_mean) / heavy_std
        zscore_labels = (np.abs(np.nan_to_num(heavy_zscores)) > 2.0).astype(np.int32)
        metrics = compute_metrics(zscore_labels, iforest_labels)
        mlflow.log_metrics(metrics)

        # Log anomaly details artifact
        _log_anomaly_details(features, zscore_labels, iforest_labels)

        # Register plain IsolationForest model (no baseline.json — not needed)
        signature = infer_signature(X, iforest_labels)
        mlflow.sklearn.log_model(
            detector.model,
            artifact_path="isolation_forest",
            signature=signature,
            input_example=X[:5],
            registered_model_name=registered_name,
        )

        logger.info(
            "route=%s: trained %d samples, iforest=%d anomalies (%.1f%%), agreement=%.3f",
            route_id, features.num_rows, int(iforest_labels.sum()),
            100.0 * iforest_labels.sum() / len(iforest_labels), metrics["model_agreement"],
        )
        return {
            "route_id": route_id,
            "status": "completed",
            "sample_count": features.num_rows,
            "iforest_anomalies": int(iforest_labels.sum()),
        }


def run_training() -> TrainResult:
    """Train one IsolationForest per route. Returns a TrainResult."""
    logger.info("Starting per-route ML training pipeline")

    # --- Setup ---
    configure_mlflow(settings.mlflow_tracking_uri)
    experiment_id = get_or_create_experiment(_EXPERIMENT_NAME)
    mlflow.set_experiment(experiment_id=experiment_id)

    catalog = get_iceberg_catalog(
        catalog_uri=settings.iceberg_catalog_uri,
        minio_endpoint=settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
    )

    route_ids = list_route_ids(catalog)
    if not route_ids:
        logger.warning("No routes found in gold table — ensure batch pipeline has run")
        return TrainResult(run_id="", status="skipped", sample_count=0, metrics={})

    logger.info("Training per-route models for %d routes", len(route_ids))

    # --- Parent run wraps all per-route child runs ---
    with mlflow.start_run(
        run_name=f"per-route-train-{datetime.now(timezone.utc):%Y%m%d-%H%M}",
        log_system_metrics=True,
        tags={
            "pipeline": "per-route-iforest",
            "data.source": "gold.traffic_hourly",
            "data.routes": str(len(route_ids)),
        },
    ) as parent_run:
        mlflow.log_params({
            "route_count": len(route_ids),
            "feature_columns": FEATURE_COLUMNS,
            "feature_count": len(FEATURE_COLUMNS),
        })

        results = [
            _train_single_route(rid, catalog, parent_run.info.run_id)
            for rid in sorted(route_ids)
        ]

        completed = [r for r in results if r["status"] == "completed"]
        total_samples = sum(int(r["sample_count"]) for r in completed)  # type: ignore[call-overload]

        mlflow.log_metrics({
            "routes_trained": len(completed),
            "routes_skipped": len(results) - len(completed),
            "total_samples": total_samples,
        })

        logger.info(
            "Per-route training complete — %d/%d routes trained, %d total samples",
            len(completed), len(route_ids), total_samples,
        )

        return TrainResult(
            run_id=parent_run.info.run_id,
            status="completed",
            sample_count=total_samples,
            metrics={"routes_trained": float(len(completed))},
        )


if __name__ == "__main__":
    result = run_training()
    if result.status == "skipped":
        sys.exit(0)
