"""Entry point for model training and MLflow experiment tracking.

Reads gold + baseline Iceberg tables, engineers features, trains
Z-score and IsolationForest detectors, logs everything to MLflow.
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
from ml.features.traffic_features import FEATURE_COLUMNS, build_features
from ml.models.isolation_forest import IsolationForestDetector
from ml.models.zscore_detector import ZScoreDetector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

_EXPERIMENT_NAME = "traffic-anomaly-detection"

# Enable sklearn autologging — automatically captures model params, metrics,
# and serialized model. We disable model logging here because we do it manually
# with a custom signature and registered model name.
mlflow.sklearn.autolog(log_models=False, silent=True)


def _log_dataset(features: pa.Table) -> None:
    """Log the feature table as an MLflow dataset input."""
    df = features.select(FEATURE_COLUMNS).to_pandas()
    dataset = mlflow.data.from_pandas(df, name="traffic_features", targets=None)
    mlflow.log_input(dataset, context="training")


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


def run_training() -> TrainResult:
    """Execute the full training pipeline. Returns a TrainResult."""
    logger.info("Starting ML training pipeline")

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

    # --- Feature Engineering ---
    features = build_features(catalog)
    if features is None or features.num_rows == 0:
        logger.warning("No features available — ensure gold and baseline tables have data")
        return TrainResult(run_id="", status="skipped", sample_count=0, metrics={})

    logger.info("Feature table: %d rows", features.num_rows)
    unique_routes = len(set(features.column("route_id").to_pylist()))

    # --- Single run with system metrics ---
    with mlflow.start_run(
        run_name=f"train-{datetime.now(timezone.utc):%Y%m%d-%H%M}",
        log_system_metrics=True,
        tags={
            "mlflow.note.content": (
                "Traffic anomaly detection training run.\n"
                f"Data: {features.num_rows} samples, {unique_routes} routes, "
                f"{len(FEATURE_COLUMNS)} features.\n"
                "Models: Z-score (threshold) + IsolationForest (unsupervised)."
            ),
            "pipeline": "traffic-anomaly-detection",
            "data.source": "gold.traffic_hourly + gold.traffic_baseline",
            "data.rows": str(features.num_rows),
            "data.routes": str(unique_routes),
        },
    ) as run:
        # --- Dataset input tracking ---
        _log_dataset(features)

        # --- Parameters ---
        mlflow.log_params({
            "feature_columns": FEATURE_COLUMNS,
            "feature_count": len(FEATURE_COLUMNS),
            "sample_count": features.num_rows,
            "unique_routes": unique_routes,
        })

        # --- Feature distribution stats ---
        _log_feature_stats(features)

        # --- Z-Score Detector ---
        zscore = ZScoreDetector(threshold=3.0)
        mlflow.log_params(zscore.get_params())
        zscore_labels = zscore.predict(features)
        mlflow.log_metrics({
            "zscore_anomaly_count": int(zscore_labels.sum()),
            "zscore_anomaly_rate": float(zscore_labels.sum()) / len(zscore_labels),
        })

        # --- Isolation Forest ---
        iforest = IsolationForestDetector(contamination=0.05, n_estimators=100)
        mlflow.log_params(iforest.get_params())
        iforest.fit(features)
        iforest_labels = iforest.predict(features)
        scores = iforest.decision_scores(features)

        mlflow.log_metrics({
            "iforest_anomaly_count": int(iforest_labels.sum()),
            "iforest_anomaly_rate": float(iforest_labels.sum()) / len(iforest_labels),
            "iforest_score_mean": float(np.mean(scores)),
            "iforest_score_std": float(np.std(scores)),
            "iforest_score_p5": float(np.percentile(scores, 5)),
            "iforest_score_p95": float(np.percentile(scores, 95)),
        })

        # --- Log model with signature + input example ---
        X = iforest._to_numpy(features)
        signature = infer_signature(X, iforest_labels)
        input_example = X[:5]  # first 5 rows as serving reference
        mlflow.sklearn.log_model(
            iforest.model,
            artifact_path="isolation_forest",
            signature=signature,
            input_example=input_example,
            registered_model_name="traffic-anomaly-iforest",
        )

        # --- Cross-model evaluation ---
        metrics = compute_metrics(zscore_labels, iforest_labels)
        mlflow.log_metrics(metrics)

        # --- Diagnostic artifacts ---
        _log_anomaly_details(features, zscore_labels, iforest_labels)

        logger.info(
            "Training complete — run=%s, zscore=%d, iforest=%d, agreement=%.3f",
            run.info.run_id,
            int(zscore_labels.sum()),
            int(iforest_labels.sum()),
            metrics["model_agreement"],
        )

        return TrainResult(
            run_id=run.info.run_id,
            status="completed",
            sample_count=features.num_rows,
            metrics=metrics,
        )


if __name__ == "__main__":
    result = run_training()
    if result.status == "skipped":
        sys.exit(0)
