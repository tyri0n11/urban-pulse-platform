"""Stream processor for real-time weather events."""

import io
import json
import time
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Message
from urbanpulse_core.models.weather import WeatherObservation

from logger import Logger
from processors.base import BaseProcessor
from sinks.minio import MinioClient

BUFFER_SIZE = 10
FLUSH_INTERVAL_S = 30


class WeatherProcessor(BaseProcessor):
    BUCKET = "urban-pulse"
    TOPIC = "weather-hcmc-bronze"

    def __init__(self, minio: MinioClient) -> None:
        self.minio = minio
        self.logger = Logger("processor.weather")
        self._buffer: list[WeatherObservation] = []
        self.last_flush_time: float = time.monotonic()

    def process(self, message: Message) -> bool:
        raw: bytes = message.value()
        obs = WeatherObservation.model_validate(json.loads(raw))
        self._buffer.append(obs)
        self.logger.info(
            f"location={obs.location_id} hour_utc={obs.hour_utc} "
            f"weather={obs.weather_desc} buffer_size={len(self._buffer)}"
        )
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

        batch = self._buffer
        ts = batch[0].hour_utc
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
        self.last_flush_time = time.monotonic()
        self.logger.info(f"Flushed {len(batch)} weather records → {object_name}")
        return True

    def on_error(self, message: Message, error: Exception) -> None:
        self.logger.error(f"Failed to process weather message offset={message.offset()}: {error}")


def _to_parquet(batch: list[WeatherObservation]) -> bytes:
    table = pa.table({
        "location_id": [o.location_id for o in batch],
        "hour_utc": pa.array([o.hour_utc for o in batch], type=pa.timestamp("us", tz="UTC")),
        "temperature_c": [o.temperature_c for o in batch],
        "precipitation_mm": [o.precipitation_mm for o in batch],
        "rain_mm": [o.rain_mm for o in batch],
        "wind_speed_kmh": [o.wind_speed_kmh for o in batch],
        "wind_direction_deg": [o.wind_direction_deg for o in batch],
        "wind_direction_name": [o.wind_direction_name for o in batch],
        "cloud_cover_pct": [o.cloud_cover_pct for o in batch],
        "weather_code": pa.array([o.weather_code for o in batch], type=pa.int32()),
        "weather_desc": [o.weather_desc for o in batch],
        "source": [o.source for o in batch],
    })
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()
