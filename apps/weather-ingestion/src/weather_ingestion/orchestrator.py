"""Orchestrates the 15-minute weather ingestion cycle across all 6 zones."""

import logging
import time

from weather_ingestion.publishers.kafka import WeatherKafkaPublisher
from weather_ingestion.sources.openmeteo import fetch_all_zones

logger = logging.getLogger(__name__)


def run(publisher: WeatherKafkaPublisher) -> None:
    """Fetch current-hour weather for all 6 zones and publish each to Kafka."""
    poll_ts_ms = int(time.time() * 1000)
    observations = fetch_all_zones()

    if not observations:
        logger.warning("weather_orchestrator: no observations returned — skipping publish")
        return

    for obs in observations:
        publisher.publish(obs, poll_ts_ms=poll_ts_ms)

    logger.info(
        "weather_orchestrator: published %d zone observations (hour=%s)",
        len(observations),
        observations[0].hour_utc if observations else "N/A",
    )
