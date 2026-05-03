"""Kafka publisher for streaming ingested traffic events."""

import json
from datetime import datetime
from typing import Any

from urbanpulse_core.config import settings
from urbanpulse_infra.kafka import KafkaProducer
from traffic_ingestion.publishers import TRAFFIC_TOPIC


class KafkaPublisher:
    def __init__(self) -> None:
        self._producer = KafkaProducer(settings.kafka_bootstrap_servers)

    def publish(
        self,
        route_id: str,
        polled_at_ms: int,
        timestamp_utc: datetime,
        raw_response: dict[str, Any],
    ) -> None:
        payload = json.dumps(raw_response, ensure_ascii=False).encode()
        headers = {
            "route_id": route_id.encode(),
            "timestamp_utc": timestamp_utc.isoformat().encode(),
            "ingest_ts": str(polled_at_ms).encode(),
        }
        self._producer.produce(TRAFFIC_TOPIC, key=route_id, value=payload, headers=headers)

    def close(self) -> None:
        self._producer.flush()
