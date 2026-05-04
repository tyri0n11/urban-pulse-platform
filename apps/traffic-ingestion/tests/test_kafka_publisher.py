"""Tests for the Kafka publisher."""
import json
from unittest.mock import patch

import pytest

from traffic_ingestion.publishers import TRAFFIC_TOPIC
from traffic_ingestion.publishers.kafka import KafkaPublisher


@pytest.fixture
def publisher_and_producer():
    """KafkaPublisher with a mocked producer. Yields (publisher, mock_producer)."""
    with patch("traffic_ingestion.publishers.kafka.KafkaProducer") as MockProducer:
        pub = KafkaPublisher()
        yield pub, MockProducer.return_value


@pytest.mark.unit
class TestKafkaPublisher:
    def test_publish_calls_produce_once(
        self, publisher_and_producer, sample_route_id, sample_polled_at_ms,
        sample_timestamp_utc, sample_raw_response,
    ):
        pub, mock_producer = publisher_and_producer
        pub.publish(sample_route_id, sample_polled_at_ms, sample_timestamp_utc, sample_raw_response)
        mock_producer.produce.assert_called_once()

    def test_publish_uses_correct_topic(
        self, publisher_and_producer, sample_route_id, sample_polled_at_ms,
        sample_timestamp_utc, sample_raw_response,
    ):
        pub, mock_producer = publisher_and_producer
        pub.publish(sample_route_id, sample_polled_at_ms, sample_timestamp_utc, sample_raw_response)
        args, _ = mock_producer.produce.call_args
        assert args[0] == TRAFFIC_TOPIC

    def test_publish_uses_route_id_as_key(
        self, publisher_and_producer, sample_route_id, sample_polled_at_ms,
        sample_timestamp_utc, sample_raw_response,
    ):
        pub, mock_producer = publisher_and_producer
        pub.publish(sample_route_id, sample_polled_at_ms, sample_timestamp_utc, sample_raw_response)
        _, kwargs = mock_producer.produce.call_args
        assert kwargs["key"] == sample_route_id

    def test_publish_value_is_raw_response_json(
        self, publisher_and_producer, sample_route_id, sample_polled_at_ms,
        sample_timestamp_utc, sample_raw_response,
    ):
        pub, mock_producer = publisher_and_producer
        pub.publish(sample_route_id, sample_polled_at_ms, sample_timestamp_utc, sample_raw_response)
        _, kwargs = mock_producer.produce.call_args
        parsed = json.loads(kwargs["value"])
        assert "paths" in parsed

    def test_publish_header_ingest_ts_matches_polled_at_ms(
        self, publisher_and_producer, sample_route_id, sample_polled_at_ms,
        sample_timestamp_utc, sample_raw_response,
    ):
        pub, mock_producer = publisher_and_producer
        pub.publish(sample_route_id, sample_polled_at_ms, sample_timestamp_utc, sample_raw_response)
        _, kwargs = mock_producer.produce.call_args
        assert kwargs["headers"]["ingest_ts"] == str(sample_polled_at_ms).encode()

    def test_publish_header_contains_route_id(
        self, publisher_and_producer, sample_route_id, sample_polled_at_ms,
        sample_timestamp_utc, sample_raw_response,
    ):
        pub, mock_producer = publisher_and_producer
        pub.publish(sample_route_id, sample_polled_at_ms, sample_timestamp_utc, sample_raw_response)
        _, kwargs = mock_producer.produce.call_args
        assert kwargs["headers"]["route_id"] == sample_route_id.encode()

    def test_close_flushes_producer(self, publisher_and_producer):
        pub, mock_producer = publisher_and_producer
        pub.close()
        mock_producer.flush.assert_called_once()
