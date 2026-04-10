"""Tests for TrafficProcessor and _to_parquet."""
import io
import time
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pyarrow.parquet as pq
import pytest

from urbanpulse_core.models.traffic import CongestionMetrics, TrafficRouteObservation

# Bare imports work because conftest.py adds src/streaming to sys.path
from processors.traffic import (
    BUFFER_SIZE,
    FLUSH_INTERVAL_S,
    TrafficProcessor,
    _to_parquet,
)


# ── helpers ───────────────────────────────────────────────────────────────────

def make_observation(
    route_id: str = "zone1_urban_core_to_zone4_southern_port",
    ts: datetime | None = None,
) -> TrafficRouteObservation:
    return TrafficRouteObservation(
        route_id=route_id,
        origin="Urban Core",
        destination="Southern Port",
        distance_meters=5000.0,
        duration_ms=300000.0,
        duration_minutes=5.0,
        congestion=CongestionMetrics(
            heavy_ratio=0.1,
            moderate_ratio=0.3,
            low_ratio=0.6,
            severe_segments=0,
            total_segments=10,
        ),
        timestamp_utc=ts or datetime(2026, 4, 10, 8, 0, 0, tzinfo=timezone.utc),
    )


def make_kafka_message(
    obs: TrafficRouteObservation,
    ingest_ts_ms: int | None = None,
) -> MagicMock:
    msg = MagicMock()
    msg.value.return_value = obs.model_dump_json().encode()
    if ingest_ts_ms is not None:
        msg.headers.return_value = [("ingest_ts", str(ingest_ts_ms).encode())]
    else:
        msg.headers.return_value = []
    msg.offset.return_value = 0
    return msg


@pytest.fixture
def minio() -> MagicMock:
    return MagicMock()


@pytest.fixture
def processor(minio: MagicMock) -> TrafficProcessor:
    return TrafficProcessor(minio=minio)


# ── process() ────────────────────────────────────────────────────────────────

@pytest.mark.unit
class TestProcess:
    def test_single_message_buffered_no_flush(self, processor, minio):
        obs = make_observation()
        result = processor.process(make_kafka_message(obs))
        assert result is False
        assert len(processor._buffer) == 1
        minio.upload_bytes.assert_not_called()

    def test_message_stored_correctly_in_buffer(self, processor):
        obs = make_observation("zone2_eastern_to_zone5_western")
        processor.process(make_kafka_message(obs))
        assert processor._buffer[0].route_id == "zone2_eastern_to_zone5_western"

    def test_flush_triggered_exactly_at_capacity(self, processor, minio):
        obs = make_observation()
        result = None
        for _ in range(BUFFER_SIZE):
            result = processor.process(make_kafka_message(obs))
        assert result is True
        minio.upload_bytes.assert_called_once()

    def test_no_flush_one_below_capacity(self, processor, minio):
        obs = make_observation()
        for _ in range(BUFFER_SIZE - 1):
            processor.process(make_kafka_message(obs))
        minio.upload_bytes.assert_not_called()

    def test_buffer_cleared_after_capacity_flush(self, processor):
        obs = make_observation()
        for _ in range(BUFFER_SIZE):
            processor.process(make_kafka_message(obs))
        assert processor._buffer == []

    def test_ingest_ts_header_parsed_without_error(self, processor):
        obs = make_observation()
        ingest_ts = int(time.time() * 1000) - 500
        msg = make_kafka_message(obs, ingest_ts_ms=ingest_ts)
        processor.process(msg)  # must not raise
        assert len(processor._buffer) == 1

    def test_missing_ingest_ts_header_handled_gracefully(self, processor):
        obs = make_observation()
        msg = make_kafka_message(obs, ingest_ts_ms=None)
        processor.process(msg)
        assert len(processor._buffer) == 1


# ── flush() ───────────────────────────────────────────────────────────────────

@pytest.mark.unit
class TestFlush:
    def test_flush_empty_buffer_returns_false(self, processor, minio):
        assert processor.flush() is False
        minio.upload_bytes.assert_not_called()

    def test_flush_non_empty_returns_true(self, processor):
        processor._buffer.append(make_observation())
        assert processor.flush() is True

    def test_flush_uploads_to_correct_bucket(self, processor, minio):
        processor._buffer.append(make_observation())
        processor.flush()
        bucket = minio.upload_bytes.call_args[0][0]
        assert bucket == "urban-pulse"

    def test_flush_object_path_is_partitioned_by_date(self, processor, minio):
        # timestamp_utc = 2026-04-10 08:00 UTC
        processor._buffer.append(make_observation())
        processor.flush()
        obj_name: str = minio.upload_bytes.call_args[0][1]
        assert "bronze/traffic-route-bronze/" in obj_name
        assert "year=2026" in obj_name
        assert "month=04" in obj_name
        assert "day=10" in obj_name
        assert "hour=08" in obj_name
        assert obj_name.endswith(".parquet")

    def test_flush_uploads_valid_parquet_bytes(self, processor, minio):
        processor._buffer.append(make_observation())
        processor.flush()
        data: bytes = minio.upload_bytes.call_args[0][2]
        table = pq.read_table(io.BytesIO(data))
        assert table.num_rows == 1

    def test_flush_clears_buffer_after_success(self, processor):
        processor._buffer.extend([make_observation(), make_observation()])
        processor.flush()
        assert processor._buffer == []

    def test_flush_does_not_clear_buffer_on_minio_error(self, processor, minio):
        minio.upload_bytes.side_effect = Exception("MinIO unreachable")
        processor._buffer.append(make_observation())
        with pytest.raises(Exception, match="MinIO unreachable"):
            processor.flush()
        # Data must be preserved for retry
        assert len(processor._buffer) == 1

    def test_flush_updates_last_flush_time(self, processor):
        processor._buffer.append(make_observation())
        before = processor.last_flush_time
        processor.flush()
        assert processor.last_flush_time >= before


# ── check_time_flush() ────────────────────────────────────────────────────────

@pytest.mark.unit
class TestCheckTimeFlush:
    def test_no_flush_when_interval_not_elapsed(self, processor, minio):
        processor._buffer.append(make_observation())
        # last_flush_time was set at construction — interval hasn't elapsed
        assert processor.check_time_flush() is False
        minio.upload_bytes.assert_not_called()

    def test_flushes_when_interval_elapsed(self, processor, minio):
        processor._buffer.append(make_observation())
        processor.last_flush_time = time.monotonic() - FLUSH_INTERVAL_S - 1
        assert processor.check_time_flush() is True
        minio.upload_bytes.assert_called_once()

    def test_empty_buffer_never_flushes_even_after_interval(self, processor, minio):
        processor.last_flush_time = time.monotonic() - FLUSH_INTERVAL_S - 1
        assert processor.check_time_flush() is False
        minio.upload_bytes.assert_not_called()


# ── on_error() ────────────────────────────────────────────────────────────────

@pytest.mark.unit
class TestOnError:
    def test_on_error_logs_without_raising(self, processor):
        msg = MagicMock()
        msg.offset.return_value = 42
        # BaseProcessor.on_error raises by default; TrafficProcessor overrides to log only
        processor.on_error(msg, ValueError("bad message"))  # must not raise


# ── _to_parquet() ─────────────────────────────────────────────────────────────

@pytest.mark.unit
class TestToParquet:
    def test_output_is_bytes(self):
        data = _to_parquet([make_observation()])
        assert isinstance(data, bytes)

    def test_schema_has_all_expected_columns(self):
        data = _to_parquet([make_observation()])
        table = pq.read_table(io.BytesIO(data))
        expected = {
            "route_id", "origin", "destination", "distance_meters",
            "duration_ms", "duration_minutes", "heavy_ratio", "moderate_ratio",
            "low_ratio", "severe_segments", "total_segments", "timestamp_utc", "source",
        }
        assert set(table.column_names) == expected

    def test_values_match_observation(self):
        obs = make_observation("zone3_northern_to_zone6_coastal")
        table = pq.read_table(io.BytesIO(_to_parquet([obs])))
        assert table.column("route_id")[0].as_py() == "zone3_northern_to_zone6_coastal"
        assert table.column("heavy_ratio")[0].as_py() == pytest.approx(0.1)
        assert table.column("distance_meters")[0].as_py() == pytest.approx(5000.0)

    def test_null_congestion_produces_null_columns(self):
        obs = make_observation()
        obs = obs.model_copy(update={"congestion": None})
        table = pq.read_table(io.BytesIO(_to_parquet([obs])))
        assert table.column("heavy_ratio")[0].as_py() is None
        assert table.column("severe_segments")[0].as_py() is None
        assert table.column("total_segments")[0].as_py() is None

    def test_multiple_observations_correct_row_count(self):
        batch = [make_observation(f"route_{i}") for i in range(5)]
        table = pq.read_table(io.BytesIO(_to_parquet(batch)))
        assert table.num_rows == 5

    def test_timestamp_timezone_preserved(self):
        import pyarrow as pa

        obs = make_observation()
        table = pq.read_table(io.BytesIO(_to_parquet([obs])))
        ts_type = table.schema.field("timestamp_utc").type
        assert pa.types.is_timestamp(ts_type)
        assert ts_type.tz == "UTC"
