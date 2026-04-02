"""Stream processor for real-time traffic events."""
import io
import json
import time
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Message
from urbanpulse_core.models.traffic import TrafficRouteObservation

from logger import Logger
from processors.base import BaseProcessor
from sinks.minio import MinioClient

BUFFER_SIZE = 500
FLUSH_INTERVAL_S = 30


class TrafficProcessor(BaseProcessor):
    BUCKET = "urban-pulse"
    TOPIC = "traffic-route-bronze"

    def __init__(self, minio: MinioClient) -> None:
        self.minio = minio
        self.logger = Logger("processor.traffic")
        self._buffer: list[TrafficRouteObservation] = []
        self.last_flush_time: float = time.monotonic()

    def process(self, message: Message) -> bool:
        t_start = time.monotonic()
        raw: bytes = message.value()
        observation = TrafficRouteObservation.model_validate(json.loads(raw))

        # Compute end-to-end latency from ingest_ts header
        latency_e2e_ms = 0
        headers = message.headers()
        if headers:
            for key, val in headers:
                if key == "ingest_ts" and val is not None:
                    ingest_ts = int(val.decode())
                    latency_e2e_ms = int(time.time() * 1000) - ingest_ts
                    break

        self._buffer.append(observation)

        latency_processing_ms = int((time.monotonic() - t_start) * 1000)
        self.logger.info(
            f"route={observation.route_id} "
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
        """Write buffered records to MinIO and clear the buffer.

        Returns True if data was successfully written.
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

        # Clear buffer only after successful upload
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


def _to_parquet(batch: list[TrafficRouteObservation]) -> bytes:
    congestion_list = [obs.congestion for obs in batch]

    table = pa.table(
        {
            "route_id": [obs.route_id for obs in batch],
            "origin": [obs.origin for obs in batch],
            "destination": [obs.destination for obs in batch],
            "distance_meters": [obs.distance_meters for obs in batch],
            "duration_ms": [obs.duration_ms for obs in batch],
            "duration_minutes": [obs.duration_minutes for obs in batch],
            "heavy_ratio": [c.heavy_ratio if c else None for c in congestion_list],
            "moderate_ratio": [c.moderate_ratio if c else None for c in congestion_list],
            "low_ratio": [c.low_ratio if c else None for c in congestion_list],
            "severe_segments": [c.severe_segments if c else None for c in congestion_list],
            "total_segments": [c.total_segments if c else None for c in congestion_list],
            "timestamp_utc": pa.array(
                [obs.timestamp_utc for obs in batch], type=pa.timestamp("us", tz="UTC")
            ),
            "source": [obs.source for obs in batch],
        }
    )

    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()
