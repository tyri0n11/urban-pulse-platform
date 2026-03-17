"""Entry point for the batch processing service."""

import logging
from datetime import timedelta

from prefect import serve

from batch.pipeline import bootstrap, traffic_pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)




def main() -> None:
    logger.info("Starting batch service — serving deployments")
    serve(
        traffic_pipeline.to_deployment(
            name="traffic-pipeline-deployment",
            interval=timedelta(minutes=15),
        ),
        
        bootstrap.to_deployment(
            name="bootstrap-deployment",
        ),
    )


if __name__ == "__main__":
    main()
