# main.py
import signal

from urbanpulse_core.config import settings
from urbanpulse_infra.kafka import KafkaProducer

from consumers.kafka import KafkaConsumer
from logger import Logger
from processors.traffic import TrafficProcessor
from processors.weather import WeatherProcessor
from sinks.minio import MinioClient

running = True
DLQ_SUFFIX = "-dlq"


def shutdown(sig=None, frame=None) -> None:  # noqa: ARG001
    global running
    Logger("streaming.main").info("Shutting down...")
    running = False


def _publish_to_dlq(
    producer: KafkaProducer,
    msg: object,
    error: Exception,
    logger: Logger,
) -> None:
    """Best-effort publish of a failed message to the dead-letter topic."""
    try:
        dlq_topic = msg.topic() + DLQ_SUFFIX  # type: ignore[union-attr]
        key = msg.key().decode() if msg.key() else "unknown"  # type: ignore[union-attr]
        producer.produce(
            topic=dlq_topic,
            key=key,
            value=msg.value(),  # type: ignore[union-attr]
            headers={"error": str(error).encode()},
        )
    except Exception as dlq_err:
        logger.error(f"Failed to publish to DLQ: {dlq_err}")


def main() -> None:
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    logger = Logger("streaming.main")
    minio = MinioClient()
    dlq_producer = KafkaProducer(settings.kafka_bootstrap_servers)

    consumer = KafkaConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id="streaming-group",
        topics=["traffic-route-bronze", "weather-hcmc-bronze"],
    )

    processors = {
        "traffic-route-bronze": TrafficProcessor(minio=minio),
        "weather-hcmc-bronze": WeatherProcessor(minio=minio),
    }

    consumer.start()

    try:
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                for processor in processors.values():
                    if processor.check_time_flush():
                        consumer.commit()
                continue

            processor = processors.get(msg.topic())
            if processor:
                try:
                    flushed = processor.process(msg)
                    if flushed:
                        consumer.commit()
                except Exception as e:
                    processor.on_error(msg, e)
                    _publish_to_dlq(dlq_producer, msg, e, logger)
    finally:
        # Drain buffers and commit before closing
        for processor in processors.values():
            if processor.flush():
                consumer.commit()
        dlq_producer.flush()
        consumer.stop()


if __name__ == "__main__":
    main()
