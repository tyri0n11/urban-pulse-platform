"""Entry point for the ingestion service."""

import logging

from urbanpulse_core.config import settings
from ingestion.orchestrator import run_once
from ingestion.publishers import Publisher, StdoutPublisher, TRAFFIC_TOPIC
from ingestion.publishers.kafka import KafkaPublisher

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    publisher: Publisher
    if settings.dry_run:
        publisher = StdoutPublisher()
        logger.info(
            "DRY_RUN=true — publishing to stdout (topic=%s), Kafka not required",
            TRAFFIC_TOPIC,
        )
    else:
        publisher = KafkaPublisher()
        logger.info("Publishing to Kafka topic=%s", TRAFFIC_TOPIC)

    try:
        run_once(publisher, api_key=settings.vietmap_api_key)
    finally:
        publisher.close()


if __name__ == "__main__":
    main()
