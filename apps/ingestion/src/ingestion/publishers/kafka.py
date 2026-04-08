"""Kafka publisher for streaming ingested events to topics."""

import time

from urbanpulse_core.config import settings
from urbanpulse_core.models.traffic import TrafficRouteObservation
from urbanpulse_infra.kafka import KafkaProducer
from ingestion.publishers import TRAFFIC_TOPIC


class KafkaPublisher:
    def __init__(self) -> None:
        self._producer = KafkaProducer(settings.kafka_bootstrap_servers)

    def publish(self, observation: TrafficRouteObservation, poll_ts_ms: int | None = None) -> None:
        payload = observation.model_dump_json().encode()
        # Use poll_ts_ms (before API call) when available so that last_ingest_lag_ms
        # in the online service measures the full raw-to-feature pipeline latency.
        ts = poll_ts_ms if poll_ts_ms is not None else int(time.time() * 1000)
        headers = {"ingest_ts": str(ts).encode()}
        self._producer.produce(
            TRAFFIC_TOPIC, key=observation.route_id, value=payload, headers=headers
        )

    def close(self) -> None:
        self._producer.flush()
