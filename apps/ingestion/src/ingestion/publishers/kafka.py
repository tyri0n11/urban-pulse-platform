"""Kafka publisher for streaming ingested events to topics."""

from urbanpulse_core.models.traffic import TrafficRouteObservation
from urbanpulse_infra.kafka import KafkaProducer
from ingestion.publishers import TRAFFIC_TOPIC

_BOOTSTRAP_SERVERS = "localhost:19092"


class KafkaPublisher:
    def __init__(self) -> None:
        self._producer = KafkaProducer(_BOOTSTRAP_SERVERS)

    def publish(self, observation: TrafficRouteObservation) -> None:
        payload = observation.model_dump_json().encode()
        self._producer.produce(TRAFFIC_TOPIC, key=observation.route_id, value=payload)

    def close(self) -> None:
        self._producer.flush()
