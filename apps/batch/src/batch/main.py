"""Entry point for the batch processing service."""

import logging
from datetime import timedelta

from prefect import serve

from batch.pipeline import backfill, bootstrap, hourly_gold, microbatch, retrain

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
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
