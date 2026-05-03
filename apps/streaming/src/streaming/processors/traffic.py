"""Stream processor for real-time traffic events — saves raw VietMap envelopes to MinIO."""
import io
import json
import time
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Message
from urbanpulse_core.models.traffic import VietmapRawEnvelope

from logger import Logger
from processors.base import BaseProcessor
from sinks.minio import MinioClient

BUFFER_SIZE = 500
FLUSH_INTERVAL_S = 30


class TrafficProcessor(BaseProcessor):
    BUCKET = "urban-pulse"
    TOPIC = "vietmap-raw"

    def __init__(self, minio: MinioClient) -> None:
        self.minio = minio
        self.logger = Logger("processor.traffic")
        self._buffer: list[VietmapRawEnvelope] = []
        self.last_flush_time: float = time.monotonic()

    def process(self, message: Message) -> bool:
        t_start = time.monotonic()
        raw: bytes = message.value()
        envelope = VietmapRawEnvelope.model_validate(json.loads(raw))

        # Compute end-to-end latency from ingest_ts header
        latency_e2e_ms = 0
        headers = message.headers()
        if headers:
            for key, val in headers:
                if key == "ingest_ts" and val is not None:
                    try:
                        latency_e2e_ms = int(time.time() * 1000) - int(val.decode())
                    except (ValueError, TypeError):
                        pass
                    break

        self._buffer.append(envelope)

        latency_processing_ms = int((time.monotonic() - t_start) * 1000)
        self.logger.info(
            f"route={envelope.route_id} "
            f"latency_e2e_ms={latency_e2e_ms} "
            f"latency_processing_ms={latency_processing_ms} "
            f"buffer_size={len(self._buffer)} buffer_capacity={BUFFER_SIZE}"
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
        """Write buffered raw envelopes to MinIO as Parquet and clear the buffer.

        Buffer is only cleared after a successful upload to prevent data loss.
        """
        if not self._buffer:
            return False

        t_start = time.monotonic()
        batch = self._buffer

        ts = batch[0].timestamp_utc
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
            f"Flushed {len(batch)} records → {object_name} "
            f"latency_flush_ms={latency_flush_ms} records={len(batch)}"
        )
        return True

    def on_error(self, message: Message, error: Exception) -> None:
        self.logger.error(f"Failed to process message offset={message.offset()}: {error}")


def _to_parquet(batch: list[VietmapRawEnvelope]) -> bytes:
    table = pa.table(
        {
            "route_id": pa.array([e.route_id for e in batch], type=pa.string()),
            "origin": pa.array([e.origin for e in batch], type=pa.string()),
            "destination": pa.array([e.destination for e in batch], type=pa.string()),
            "polled_at_ms": pa.array([e.polled_at_ms for e in batch], type=pa.int64()),
            "timestamp_utc": pa.array(
                [e.timestamp_utc for e in batch], type=pa.timestamp("us", tz="UTC")
            ),
            "raw_response": pa.array(
                [json.dumps(e.raw_response, ensure_ascii=False) for e in batch],
                type=pa.string(),
            ),
        }
    )

    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()
