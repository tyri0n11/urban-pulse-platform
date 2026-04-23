"""Entry point for the traffic ingestion service."""

import logging
import time

from urbanpulse_core.config import settings
from traffic_ingestion.orchestrator import run
from traffic_ingestion.publishers import Publisher, StdoutPublisher, TRAFFIC_TOPIC
from traffic_ingestion.publishers.kafka import KafkaPublisher

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

_CYCLE_INTERVAL_S = 5


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
        while True:
            run(publisher, api_key=settings.vietmap_api_key)
            logger.info("Waiting %ds before next crawl cycle...", _CYCLE_INTERVAL_S)
            time.sleep(_CYCLE_INTERVAL_S)
    finally:
        publisher.close()


if __name__ == "__main__":
    main()
