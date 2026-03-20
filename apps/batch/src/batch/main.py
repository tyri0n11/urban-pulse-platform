"""Entry point for the batch processing service."""

import logging
from datetime import timedelta

from prefect import serve

from batch.pipeline import bootstrap, microbatch

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
        bootstrap.to_deployment(
            name="bootstrap-deployment",
            tags=["bootstrap"],
        ),
    )


if __name__ == "__main__":
    main()
