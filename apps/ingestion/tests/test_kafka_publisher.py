"""Tests for the Kafka publisher."""
import json
from unittest.mock import patch

import pytest

from ingestion.publishers import TRAFFIC_TOPIC
from ingestion.publishers.kafka import KafkaPublisher


@pytest.fixture
def publisher_and_producer(sample_observation):
    """KafkaPublisher with a mocked producer. Yields (publisher, mock_producer)."""
    with patch("ingestion.publishers.kafka.KafkaProducer") as MockProducer:
        pub = KafkaPublisher()
        yield pub, MockProducer.return_value


@pytest.mark.unit
class TestKafkaPublisher:
    def test_publish_calls_produce_once(self, publisher_and_producer, sample_observation):
        pub, mock_producer = publisher_and_producer
        pub.publish(sample_observation, poll_ts_ms=1000)
        mock_producer.produce.assert_called_once()

    def test_publish_uses_correct_topic(self, publisher_and_producer, sample_observation):
        pub, mock_producer = publisher_and_producer
        pub.publish(sample_observation, poll_ts_ms=1000)
        args, _ = mock_producer.produce.call_args
        assert args[0] == TRAFFIC_TOPIC

    def test_publish_uses_route_id_as_key(self, publisher_and_producer, sample_observation):
        pub, mock_producer = publisher_and_producer
        pub.publish(sample_observation, poll_ts_ms=1000)
        _, kwargs = mock_producer.produce.call_args
        assert kwargs["key"] == sample_observation.route_id

    def test_publish_value_is_valid_json_bytes(self, publisher_and_producer, sample_observation):
        pub, mock_producer = publisher_and_producer
        pub.publish(sample_observation, poll_ts_ms=1000)
        _, kwargs = mock_producer.produce.call_args
        parsed = json.loads(kwargs["value"])
        assert parsed["route_id"] == sample_observation.route_id
        assert parsed["distance_meters"] == sample_observation.distance_meters

    def test_publish_header_contains_poll_ts_ms(self, publisher_and_producer, sample_observation):
        pub, mock_producer = publisher_and_producer
        pub.publish(sample_observation, poll_ts_ms=999888777)
        _, kwargs = mock_producer.produce.call_args
        assert kwargs["headers"]["ingest_ts"] == b"999888777"

    def test_publish_uses_current_time_when_poll_ts_none(
        self, publisher_and_producer, sample_observation
    ):
        pub, mock_producer = publisher_and_producer
        with patch("ingestion.publishers.kafka.time.time", return_value=1234567.0):
            pub.publish(sample_observation, poll_ts_ms=None)
        _, kwargs = mock_producer.produce.call_args
        assert kwargs["headers"]["ingest_ts"] == b"1234567000"

    def test_close_flushes_producer(self, publisher_and_producer):
        pub, mock_producer = publisher_and_producer
        pub.close()
        mock_producer.flush.assert_called_once()
