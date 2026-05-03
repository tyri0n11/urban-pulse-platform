"""Kafka publisher for streaming ingested traffic events."""

from urbanpulse_core.config import settings
from urbanpulse_core.models.traffic import VietmapRawEnvelope
from urbanpulse_infra.kafka import KafkaProducer
from traffic_ingestion.publishers import TRAFFIC_TOPIC


class KafkaPublisher:
    def __init__(self) -> None:
        self._producer = KafkaProducer(settings.kafka_bootstrap_servers)

    def publish(self, envelope: VietmapRawEnvelope) -> None:
        payload = envelope.model_dump_json().encode()
        headers = {"ingest_ts": str(envelope.polled_at_ms).encode()}
        self._producer.produce(
            TRAFFIC_TOPIC, key=envelope.route_id, value=payload, headers=headers
        )

    def close(self) -> None:
        self._producer.flush()
