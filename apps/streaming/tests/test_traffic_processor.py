"""Tests for TrafficProcessor and _to_parquet."""
import io
import json
import time
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from urbanpulse_core.models.traffic import VietmapRawEnvelope

# Bare imports work because conftest.py adds src/streaming to sys.path
from processors.traffic import (
    BUFFER_SIZE,
    FLUSH_INTERVAL_S,
    TrafficProcessor,
    _to_parquet,
)

_SAMPLE_RAW_RESPONSE = {
    "paths": [{
        "distance": 5000.0,
        "time": 300000.0,
        "annotations": {
            "congestion": [
                {"value": "heavy", "first": 0, "last": 1},
                {"value": "moderate", "first": 1, "last": 2},
                {"value": "low", "first": 2, "last": 3},
            ]
        },
    }]
}


def make_envelope(
    route_id: str = "zone1_urban_core_to_zone4_southern_port",
    ts: datetime | None = None,
) -> VietmapRawEnvelope:
    return VietmapRawEnvelope(
        route_id=route_id,
        origin="Urban Core",
        destination="Southern Port",
        polled_at_ms=1744272000000,
        timestamp_utc=ts or datetime(2026, 4, 10, 8, 0, 0, tzinfo=timezone.utc),
        raw_response=_SAMPLE_RAW_RESPONSE,
    )


def make_kafka_message(
    envelope: VietmapRawEnvelope,
    ingest_ts_ms: int | None = None,
) -> MagicMock:
    msg = MagicMock()
    msg.value.return_value = envelope.model_dump_json().encode()
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
        result = processor.process(make_kafka_message(make_envelope()))
        assert result is False
        assert len(processor._buffer) == 1
        minio.upload_bytes.assert_not_called()

    def test_message_stored_correctly_in_buffer(self, processor):
        env = make_envelope("zone2_eastern_to_zone5_western")
        processor.process(make_kafka_message(env))
        assert processor._buffer[0].route_id == "zone2_eastern_to_zone5_western"

    def test_flush_triggered_exactly_at_capacity(self, processor, minio):
        result = None
        for _ in range(BUFFER_SIZE):
            result = processor.process(make_kafka_message(make_envelope()))
        assert result is True
        minio.upload_bytes.assert_called_once()

    def test_no_flush_one_below_capacity(self, processor, minio):
        for _ in range(BUFFER_SIZE - 1):
            processor.process(make_kafka_message(make_envelope()))
        minio.upload_bytes.assert_not_called()

    def test_buffer_cleared_after_capacity_flush(self, processor):
        for _ in range(BUFFER_SIZE):
            processor.process(make_kafka_message(make_envelope()))
        assert processor._buffer == []

    def test_ingest_ts_header_parsed_without_error(self, processor):
        ingest_ts = int(time.time() * 1000) - 500
        processor.process(make_kafka_message(make_envelope(), ingest_ts_ms=ingest_ts))
        assert len(processor._buffer) == 1

    def test_missing_ingest_ts_header_handled_gracefully(self, processor):
        processor.process(make_kafka_message(make_envelope(), ingest_ts_ms=None))
        assert len(processor._buffer) == 1


# ── flush() ───────────────────────────────────────────────────────────────────

@pytest.mark.unit
class TestFlush:
    def test_flush_empty_buffer_returns_false(self, processor, minio):
        assert processor.flush() is False
        minio.upload_bytes.assert_not_called()

    def test_flush_non_empty_returns_true(self, processor):
        processor._buffer.append(make_envelope())
        assert processor.flush() is True

    def test_flush_uploads_to_correct_bucket(self, processor, minio):
        processor._buffer.append(make_envelope())
        processor.flush()
        bucket = minio.upload_bytes.call_args[0][0]
        assert bucket == "urban-pulse"

    def test_flush_object_path_is_partitioned_by_date(self, processor, minio):
        # timestamp_utc = 2026-04-10 08:00 UTC
        processor._buffer.append(make_envelope())
        processor.flush()
        obj_name: str = minio.upload_bytes.call_args[0][1]
        assert "bronze/vietmap-raw/" in obj_name
        assert "year=2026" in obj_name
        assert "month=04" in obj_name
        assert "day=10" in obj_name
        assert "hour=08" in obj_name
        assert obj_name.endswith(".parquet")

    def test_flush_uploads_valid_parquet_bytes(self, processor, minio):
        processor._buffer.append(make_envelope())
        processor.flush()
        data: bytes = minio.upload_bytes.call_args[0][2]
        table = pq.read_table(io.BytesIO(data))
        assert table.num_rows == 1

    def test_flush_clears_buffer_after_success(self, processor):
        processor._buffer.extend([make_envelope(), make_envelope()])
        processor.flush()
        assert processor._buffer == []

    def test_flush_does_not_clear_buffer_on_minio_error(self, processor, minio):
        minio.upload_bytes.side_effect = Exception("MinIO unreachable")
        processor._buffer.append(make_envelope())
        with pytest.raises(Exception, match="MinIO unreachable"):
            processor.flush()
        assert len(processor._buffer) == 1

    def test_flush_updates_last_flush_time(self, processor):
        processor._buffer.append(make_envelope())
        before = processor.last_flush_time
        processor.flush()
        assert processor.last_flush_time >= before


# ── check_time_flush() ────────────────────────────────────────────────────────

@pytest.mark.unit
class TestCheckTimeFlush:
    def test_no_flush_when_interval_not_elapsed(self, processor, minio):
        processor._buffer.append(make_envelope())
        assert processor.check_time_flush() is False
        minio.upload_bytes.assert_not_called()

    def test_flushes_when_interval_elapsed(self, processor, minio):
        processor._buffer.append(make_envelope())
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
        processor.on_error(msg, ValueError("bad message"))


# ── _to_parquet() ─────────────────────────────────────────────────────────────

@pytest.mark.unit
class TestToParquet:
    def test_output_is_bytes(self):
        assert isinstance(_to_parquet([make_envelope()]), bytes)

    def test_schema_has_all_expected_columns(self):
        table = pq.read_table(io.BytesIO(_to_parquet([make_envelope()])))
        expected = {
            "route_id", "origin", "destination",
            "polled_at_ms", "timestamp_utc", "raw_response",
        }
        assert set(table.column_names) == expected

    def test_values_match_envelope(self):
        env = make_envelope("zone3_northern_to_zone6_coastal")
        table = pq.read_table(io.BytesIO(_to_parquet([env])))
        assert table.column("route_id")[0].as_py() == "zone3_northern_to_zone6_coastal"
        assert table.column("polled_at_ms")[0].as_py() == 1744272000000

    def test_raw_response_stored_as_json_string(self):
        env = make_envelope()
        table = pq.read_table(io.BytesIO(_to_parquet([env])))
        raw_str = table.column("raw_response")[0].as_py()
        assert isinstance(raw_str, str)
        parsed = json.loads(raw_str)
        assert "paths" in parsed

    def test_multiple_observations_correct_row_count(self):
        batch = [make_envelope(f"route_{i}") for i in range(5)]
        table = pq.read_table(io.BytesIO(_to_parquet(batch)))
        assert table.num_rows == 5

    def test_timestamp_timezone_preserved(self):
        table = pq.read_table(io.BytesIO(_to_parquet([make_envelope()])))
        ts_type = table.schema.field("timestamp_utc").type
        assert pa.types.is_timestamp(ts_type)
        assert ts_type.tz == "UTC"
