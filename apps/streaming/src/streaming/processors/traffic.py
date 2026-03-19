"""Stream processor for real-time traffic events."""
import io
import json
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Message
from urbanpulse_core.models.traffic import TrafficRouteObservation

from logger import Logger
from processors.base import BaseProcessor
from sinks.minio import MinioClient

BUFFER_SIZE = 20
BOUNDED_LATENCY_SECONDS = 20

class TrafficProcessor(BaseProcessor):
    BUCKET = "urban-pulse"
    TOPIC = "traffic-route-bronze"

    def __init__(self, minio: MinioClient) -> None:
        self.minio = minio
        self.logger = Logger("processor.traffic")
        self._buffer: list[TrafficRouteObservation] = []

    def process(self, message: Message) -> None:
        raw: bytes = message.value()
        observation = TrafficRouteObservation.model_validate(json.loads(raw))
        self.logger.info(f"Processing route {observation.route_id} with congestion {observation.congestion}")
        self._buffer.append(observation)
        self.logger.info(f"buffer_size={len(self._buffer)} buffer_capacity={BUFFER_SIZE}")

        if len(self._buffer) >= BUFFER_SIZE:
            self.flush()

    def flush(self) -> None:
        """Write buffered records to MinIO and clear the buffer."""
        if not self._buffer:
            return

        batch = self._buffer
        self._buffer = []

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
        self.logger.info(f"Flushed {len(batch)} records → {object_name}")

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
    pq.write_table(table, buf)
    return buf.getvalue()
