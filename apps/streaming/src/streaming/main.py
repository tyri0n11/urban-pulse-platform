# main.py
import signal

from urbanpulse_core.config import settings

from consumers.kafka import KafkaConsumer
from logger import Logger
from processors.traffic import TrafficProcessor
from sinks.minio import MinioClient

running = True


def shutdown(sig=None, frame=None) -> None:  # noqa: ARG001
    global running
    Logger("streaming.main").info("Shutting down...")
    running = False


def main():
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    minio = MinioClient()

    consumer = KafkaConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id="streaming-group",
        topics=["traffic-route-bronze"],
    )

    processors = {
        "traffic-route-bronze": TrafficProcessor(minio=minio),
    }

    consumer.start()

    try:
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            processor = processors.get(msg.topic())
            if processor:
                try:
                    processor.process(msg)
                    consumer.commit()
                except Exception as e:
                    processor.on_error(msg, e)
    finally:
        for processor in processors.values():
            processor.flush()
        consumer.stop()


if __name__ == "__main__":
    main()
