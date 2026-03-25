"""MLflow tracking client helpers and experiment utilities."""

import mlflow


def configure_mlflow(tracking_uri: str) -> None:
    """Set the MLflow tracking URI for all subsequent calls."""
    mlflow.set_tracking_uri(tracking_uri)


def get_or_create_experiment(name: str) -> str:
    """Return the experiment ID, creating it if it doesn't exist."""
    experiment = mlflow.get_experiment_by_name(name)
    if experiment is not None:
        return experiment.experiment_id
    return mlflow.create_experiment(name)
