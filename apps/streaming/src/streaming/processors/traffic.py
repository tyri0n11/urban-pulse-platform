"""Stream processor for real-time traffic events — saves raw Kafka messages to MinIO as Parquet."""
import io
import time
from datetime import datetime, timezone
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Message

from logger import Logger
from processors.base import BaseProcessor
from sinks.minio import MinioClient

BUFFER_SIZE = 500
FLUSH_INTERVAL_S = 30

# (raw_json, route_id, timestamp_utc_iso, ingest_ts)
_Record = tuple[str, str, str, int]


class TrafficProcessor(BaseProcessor):
    BUCKET = "urban-pulse"
    TOPIC = "vietmap-raw"

    def __init__(self, minio: MinioClient) -> None:
        self.minio = minio
        self.logger = Logger("processor.traffic")
        self._buffer: list[_Record] = []
        self.last_flush_time: float = time.monotonic()

    def process(self, message: Message) -> bool:
        raw_bytes: bytes | None = message.value()
        if raw_bytes is None:
            return False
        raw: str = raw_bytes.decode("utf-8")

        route_id = ""
        timestamp_utc = ""
        ingest_ts = 0
        for key, val in (message.headers() or []):
            if not isinstance(val, bytes):
                continue
            s = val.decode()
            if key == "route_id":
                route_id = s
            elif key == "timestamp_utc":
                timestamp_utc = s
            elif key == "ingest_ts":
                try:
                    ingest_ts = int(s)
                except (ValueError, TypeError):
                    pass

        self._buffer.append((raw, route_id, timestamp_utc, ingest_ts))
        self.logger.info(
            f"route={route_id} buffer_size={len(self._buffer)} buffer_capacity={BUFFER_SIZE}"
        )

        if len(self._buffer) >= BUFFER_SIZE:
            return self.flush()
        return False

    def check_time_flush(self) -> bool:
        """Flush if FLUSH_INTERVAL_S has elapsed since the last flush."""
        if self._buffer and (time.monotonic() - self.last_flush_time) >= FLUSH_INTERVAL_S:
            self.logger.info("Time-based flush triggered")
            return self.flush()
        return False

    def flush(self) -> bool:
        """Write buffered records to MinIO as Parquet. Buffer cleared only after successful upload."""
        if not self._buffer:
            return False

        t_start = time.monotonic()
        batch = self._buffer

        first_ingest_ts = batch[0][3]
        ts = (
            datetime.fromtimestamp(first_ingest_ts / 1000, tz=timezone.utc)
            if first_ingest_ts
            else datetime.now(timezone.utc)
        )
        object_name = (
            f"bronze/{self.TOPIC}/"
            f"year={ts.year:04d}/"
            f"month={ts.month:02d}/"
            f"day={ts.day:02d}/"
            f"hour={ts.hour:02d}/"
            f"{uuid4()}.parquet"
        )

        parquet_bytes = _to_parquet(batch)
        self.minio.upload_bytes(self.BUCKET, object_name, parquet_bytes)

        self._buffer = []
        latency_flush_ms = int((time.monotonic() - t_start) * 1000)
        self.last_flush_time = time.monotonic()
        self.logger.info(
            f"Flushed {len(batch)} records → {object_name} latency_flush_ms={latency_flush_ms}"
        )
        return True

    def on_error(self, message: Message, error: Exception) -> None:
        self.logger.error(f"Failed to process message offset={message.offset()}: {error}")


def _to_parquet(batch: list[_Record]) -> bytes:
    table = pa.table({
        "raw": pa.array([r[0] for r in batch], type=pa.string()),
        "route_id": pa.array([r[1] for r in batch], type=pa.string()),
        "timestamp_utc": pa.array([r[2] for r in batch], type=pa.string()),
        "ingest_ts": pa.array([r[3] for r in batch], type=pa.int64()),
    })
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()
