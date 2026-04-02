"""Base class definitions for stream processors."""

from abc import ABC, abstractmethod

from confluent_kafka import Message


class BaseProcessor(ABC):

    @abstractmethod
    def process(self, message: Message) -> bool:
        """Process one Kafka message. Returns True if a flush occurred."""
        ...

    def on_error(self, message: Message, error: Exception) -> None:
        """Override for custom error handling."""
        raise error

    def flush(self) -> bool:
        """Flush buffer before shutdown. Returns True if data was written."""
        return False

    def check_time_flush(self) -> bool:
        """Check if a time-based flush is needed. Returns True if flushed."""
        return False