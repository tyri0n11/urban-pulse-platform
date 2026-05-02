"""Stream processor for raw VietMap API responses."""
import io
import json
import time
from datetime import datetime, timezone
from uuid import uuid4

from confluent_kafka import Message

from logger import Logger
from processors.base import BaseProcessor
from sinks.minio import MinioClient

BUFFER_SIZE = 500
FLUSH_INTERVAL_S = 30

_BUCKET = "urban-pulse"
_TOPIC = "vietmap-raw"


class VietmapRawProcessor(BaseProcessor):
    def __init__(self, minio: MinioClient) -> None:
        self.minio = minio
        self.logger = Logger("processor.vietmap_raw")
        self._buffer: list[tuple[bytes, int]] = []
        self.last_flush_time: float = time.monotonic()

    def process(self, message: Message) -> bool:
        raw: bytes = message.value()
        route_id = message.key().decode() if message.key() else "unknown"

        ingest_ts: int = int(time.time() * 1000)
        headers = message.headers()
        if headers:
            for key, val in headers:
                if key == "ingest_ts" and val is not None:
                    ingest_ts = int(val.decode())
                    break

        record = json.dumps(
            {"route_id": route_id, "ingest_ts": ingest_ts, "raw": json.loads(raw)},
            ensure_ascii=False,
        ).encode()
        self._buffer.append((record, ingest_ts))

        if len(self._buffer) >= BUFFER_SIZE:
            return self.flush()
        return False

    def check_time_flush(self) -> bool:
        if self._buffer and (time.monotonic() - self.last_flush_time) >= FLUSH_INTERVAL_S:
            self.logger.info("Time-based flush triggered")
            return self.flush()
        return False

    def flush(self) -> bool:
        if not self._buffer:
            return False

        t_start = time.monotonic()
        batch = self._buffer
        # Use ingest_ts of first message for event-time partitioning
        ts = datetime.fromtimestamp(batch[0][1] / 1000, tz=timezone.utc)
        object_name = (
            f"bronze/{_TOPIC}/"
            f"year={ts.year:04d}/"
            f"month={ts.month:02d}/"
            f"day={ts.day:02d}/"
            f"hour={ts.hour:02d}/"
            f"{uuid4()}.ndjson"
        )

        ndjson_bytes = b"\n".join(record for record, _ in batch)
        self.minio.upload_bytes(_BUCKET, object_name, ndjson_bytes)

        self._buffer = []
        latency_flush_ms = int((time.monotonic() - t_start) * 1000)
        self.last_flush_time = time.monotonic()
        self.logger.info(
            f"Flushed {len(batch)} records → {object_name} latency_flush_ms={latency_flush_ms}"
        )
        return True

    def on_error(self, message: Message, error: Exception) -> None:
        self.logger.error(f"Failed to process message offset={message.offset()}: {error}")
