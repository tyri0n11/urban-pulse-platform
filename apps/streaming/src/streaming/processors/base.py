"""Base class definitions for stream processors."""
from abc import ABC, abstractmethod
from confluent_kafka import Message

class BaseProcessor(ABC):

    @abstractmethod
    def process(self, message: Message) -> None:
        """Xử lý 1 message từ Kafka."""
        ...

    def on_error(self, message: Message, error: Exception) -> None:
        """Override nếu muốn custom error handling / DLQ."""
        raise error

    def flush(self) -> None:
        """Xả buffer trước khi shutdown. Override nếu processor có buffer."""