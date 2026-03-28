"""Entry point for the batch processing service."""

import logging
from datetime import timedelta

from prefect import serve

from batch.pipeline import backfill, bootstrap, hourly_gold, microbatch, retrain

_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
_STREAM_HANDLER = logging.StreamHandler()
_STREAM_HANDLER.setFormatter(logging.Formatter(_LOG_FORMAT))

# Force batch module loggers to write to stdout regardless of Prefect's
# logging interception — Prefect overrides basicConfig in task workers.
for _name in (
    "batch.jobs.bronze_to_silver",
    "batch.jobs.silver_to_gold",
    "batch.jobs.baseline_learning",
    "batch.pipeline",
    "batch.main",
):
    _log = logging.getLogger(_name)
    _log.setLevel(logging.INFO)
    _log.addHandler(_STREAM_HANDLER)
    _log.propagate = False  # prevent duplicate output via root logger

logging.basicConfig(level=logging.INFO, format=_LOG_FORMAT)
logger = logging.getLogger(__name__)


def main() -> None:
    logger.info("Starting batch service — serving deployments")
    serve(
        microbatch.to_deployment(
            name="microbatch-deployment",
            interval=timedelta(minutes=5),
            tags=["scheduled", "microbatch"],
        ),
        hourly_gold.to_deployment(
            name="hourly-gold-deployment",
            interval=timedelta(hours=1),
            tags=["scheduled", "gold"],
        ),
        retrain.to_deployment(
            name="retrain-deployment",
            interval=timedelta(hours=6),
            tags=["scheduled", "retrain"],
        ),
        bootstrap.to_deployment(
            name="bootstrap-deployment",
            tags=["bootstrap"],
        ),
        backfill.to_deployment(
            name="backfill-deployment",
            tags=["backfill"],
        ),
        limit=3,
    )


if __name__ == "__main__":
    main()
