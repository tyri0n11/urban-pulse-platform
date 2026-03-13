"""Kafka/Redpanda client factory and connection helpers."""

from typing import Any


class KafkaProducer:
    """Thin wrapper around confluent_kafka.Producer."""

    def __init__(self, bootstrap_servers: str) -> None:
        from confluent_kafka import Producer  # type: ignore[import-untyped]

        self._producer: Any = Producer({"bootstrap.servers": bootstrap_servers})

    def produce(self, topic: str, key: str, value: bytes) -> None:
        self._producer.produce(topic, key=key.encode(), value=value)
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> None:
        self._producer.flush(timeout)
