"""Kafka publisher for streaming ingested events to topics."""

import time

from urbanpulse_core.config import settings
from urbanpulse_core.models.traffic import TrafficRouteObservation
from urbanpulse_infra.kafka import KafkaProducer
from ingestion.publishers import TRAFFIC_TOPIC


class KafkaPublisher:
    def __init__(self) -> None:
        self._producer = KafkaProducer(settings.kafka_bootstrap_servers)

    def publish(self, observation: TrafficRouteObservation) -> None:
        payload = observation.model_dump_json().encode()
        headers = {"ingest_ts": str(int(time.time() * 1000)).encode()}
        self._producer.produce(
            TRAFFIC_TOPIC, key=observation.route_id, value=payload, headers=headers
        )

    def close(self) -> None:
        self._producer.flush()
