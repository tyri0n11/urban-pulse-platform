"""Entry point for the batch processing service."""

import logging
from datetime import timedelta

from prefect import serve

from batch.pipeline import backfill, bootstrap, microbatch

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
            concurrency_limit=1,
        ),
        bootstrap.to_deployment(
            name="bootstrap-deployment",
            tags=["bootstrap"],
            concurrency_limit=1,
        ),
        backfill.to_deployment(
            name="backfill-deployment",
            tags=["backfill"],
            concurrency_limit=1,
        ),
    )


if __name__ == "__main__":
    main()
