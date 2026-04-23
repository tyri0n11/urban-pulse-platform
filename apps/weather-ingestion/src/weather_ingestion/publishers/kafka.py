"""Kafka publisher for weather observations."""

import time

from urbanpulse_core.config import settings
from urbanpulse_core.models.weather import WeatherObservation
from urbanpulse_infra.kafka import KafkaProducer

WEATHER_TOPIC = "weather-hcmc-bronze"


class WeatherKafkaPublisher:
    def __init__(self) -> None:
        self._producer = KafkaProducer(settings.kafka_bootstrap_servers)

    def publish(self, obs: WeatherObservation, poll_ts_ms: int | None = None) -> None:
        payload = obs.model_dump_json().encode()
        ts = poll_ts_ms if poll_ts_ms is not None else int(time.time() * 1000)
        headers = {"ingest_ts": str(ts).encode()}
        self._producer.produce(WEATHER_TOPIC, key=obs.location_id, value=payload, headers=headers)

    def close(self) -> None:
        self._producer.flush()
