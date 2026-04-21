"""Entry point for the weather ingestion service.

Polls Open-Meteo every 15 minutes for all 6 HCMC zones and publishes
to Kafka topic weather-hcmc-bronze (6 messages per cycle).
"""

import logging
import time

from weather_ingestion.orchestrator import run
from weather_ingestion.publishers.kafka import WeatherKafkaPublisher

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

_CYCLE_INTERVAL_S = 900  # 15 minutes


def main() -> None:
    logger.info(
        "Weather ingestion service starting — 6 zones, polling every %ds", _CYCLE_INTERVAL_S
    )
    publisher = WeatherKafkaPublisher()
    try:
        while True:
            run(publisher)
            logger.info("Waiting %ds before next weather poll...", _CYCLE_INTERVAL_S)
            time.sleep(_CYCLE_INTERVAL_S)
    finally:
        publisher.close()


if __name__ == "__main__":
    main()
