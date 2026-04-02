# kafka/consumer.py
from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from logger import Logger


class KafkaConsumer:
    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
        topics: list[str],
        auto_offset_reset: str = "earliest",
        enable_auto_commit: bool = False,
    ):
        self.logger = Logger("kafka.consumer")
        self.topics = topics
        self._consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.commit": enable_auto_commit,
        })

    def start(self) -> None:
        self._consumer.subscribe(self.topics)
        self.logger.info(f"Subscribed to topics: {self.topics}")

    def poll(self, timeout: float = 1.0) -> Message | None:
        msg = self._consumer.poll(timeout)

        if msg is None:
            return None

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                self.logger.debug(f"End of partition: {msg.topic()}[{msg.partition()}]")
            else:
                raise KafkaException(msg.error())
            return None

        return msg

    def commit(self) -> None:
        self._consumer.commit(asynchronous=False)

    def stop(self) -> None:
        self._consumer.close()
        self.logger.info("Kafka consumer closed.")