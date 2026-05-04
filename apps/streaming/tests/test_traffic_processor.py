"""Tests for TrafficProcessor and _to_parquet."""
import io
import json
import time
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

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

_SAMPLE_ROUTE_ID   = "zone1_urban_core_to_zone4_southern_port"
_SAMPLE_TS         = datetime(2026, 4, 10, 8, 0, 0, tzinfo=timezone.utc)
_SAMPLE_INGEST_TS  = int(_SAMPLE_TS.timestamp() * 1000)  # 2026-04-10 08:00 UTC in ms


def make_record(
    route_id: str = _SAMPLE_ROUTE_ID,
    ts: datetime | None = None,
) -> tuple[str, str, str, int]:
    """Return a _Record tuple matching the buffer format (raw, route_id, timestamp_utc, ingest_ts)."""
    effective_ts = ts or _SAMPLE_TS
    return (
        json.dumps(_SAMPLE_RAW_RESPONSE),
        route_id,
        effective_ts.isoformat(),
        _SAMPLE_INGEST_TS,
    )


def make_kafka_message(
    record: tuple[str, str, str, int],
    ingest_ts_ms: int | None = None,
) -> MagicMock:
    """Build a mock Kafka message: raw JSON body + route_id/timestamp_utc/ingest_ts headers."""
    raw, route_id, timestamp_utc, ingest_ts = record
    msg = MagicMock()
    msg.value.return_value = raw.encode()
    actual_ingest_ts = ingest_ts_ms if ingest_ts_ms is not None else ingest_ts
    msg.headers.return_value = [
        ("route_id",      route_id.encode()),
        ("timestamp_utc", timestamp_utc.encode()),
        ("ingest_ts",     str(actual_ingest_ts).encode()),
    ]
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
        result = processor.process(make_kafka_message(make_record()))
        assert result is False
        assert len(processor._buffer) == 1
        minio.upload_bytes.assert_not_called()

    def test_message_stored_correctly_in_buffer(self, processor):
        rec = make_record("zone2_eastern_to_zone5_western")
        processor.process(make_kafka_message(rec))
        assert processor._buffer[0][1] == "zone2_eastern_to_zone5_western"

    def test_flush_triggered_exactly_at_capacity(self, processor, minio):
        result = None
        for _ in range(BUFFER_SIZE):
            result = processor.process(make_kafka_message(make_record()))
        assert result is True
        minio.upload_bytes.assert_called_once()

    def test_no_flush_one_below_capacity(self, processor, minio):
        for _ in range(BUFFER_SIZE - 1):
            processor.process(make_kafka_message(make_record()))
        minio.upload_bytes.assert_not_called()

    def test_buffer_cleared_after_capacity_flush(self, processor):
        for _ in range(BUFFER_SIZE):
            processor.process(make_kafka_message(make_record()))
        assert processor._buffer == []

    def test_ingest_ts_header_parsed_without_error(self, processor):
        ingest_ts = int(time.time() * 1000) - 500
        processor.process(make_kafka_message(make_record(), ingest_ts_ms=ingest_ts))
        assert len(processor._buffer) == 1

    def test_missing_ingest_ts_header_handled_gracefully(self, processor):
        rec = make_record()
        msg = MagicMock()
        msg.value.return_value = rec[0].encode()
        msg.headers.return_value = [
            ("route_id",      rec[1].encode()),
            ("timestamp_utc", rec[2].encode()),
            # no ingest_ts header
        ]
        msg.offset.return_value = 0
        processor.process(msg)
        assert len(processor._buffer) == 1


# ── flush() ───────────────────────────────────────────────────────────────────

@pytest.mark.unit
class TestFlush:
    def test_flush_empty_buffer_returns_false(self, processor, minio):
        assert processor.flush() is False
        minio.upload_bytes.assert_not_called()

    def test_flush_non_empty_returns_true(self, processor):
        processor._buffer.append(make_record())
        assert processor.flush() is True

    def test_flush_uploads_to_correct_bucket(self, processor, minio):
        processor._buffer.append(make_record())
        processor.flush()
        bucket = minio.upload_bytes.call_args[0][0]
        assert bucket == "urban-pulse"

    def test_flush_object_path_is_partitioned_by_date(self, processor, minio):
        # ingest_ts = 1744272000000 → 2026-04-10 08:00 UTC
        processor._buffer.append(make_record())
        processor.flush()
        obj_name: str = minio.upload_bytes.call_args[0][1]
        assert "bronze/vietmap-raw/" in obj_name
        assert "year=2026" in obj_name
        assert "month=04" in obj_name
        assert "day=10" in obj_name
        assert "hour=08" in obj_name
        assert obj_name.endswith(".parquet")

    def test_flush_uploads_valid_parquet_bytes(self, processor, minio):
        processor._buffer.append(make_record())
        processor.flush()
        data: bytes = minio.upload_bytes.call_args[0][2]
        table = pq.read_table(io.BytesIO(data))
        assert table.num_rows == 1

    def test_flush_clears_buffer_after_success(self, processor):
        processor._buffer.extend([make_record(), make_record()])
        processor.flush()
        assert processor._buffer == []

    def test_flush_does_not_clear_buffer_on_minio_error(self, processor, minio):
        minio.upload_bytes.side_effect = Exception("MinIO unreachable")
        processor._buffer.append(make_record())
        with pytest.raises(Exception, match="MinIO unreachable"):
            processor.flush()
        assert len(processor._buffer) == 1

    def test_flush_updates_last_flush_time(self, processor):
        processor._buffer.append(make_record())
        before = processor.last_flush_time
        processor.flush()
        assert processor.last_flush_time >= before


# ── check_time_flush() ────────────────────────────────────────────────────────

@pytest.mark.unit
class TestCheckTimeFlush:
    def test_no_flush_when_interval_not_elapsed(self, processor, minio):
        processor._buffer.append(make_record())
        assert processor.check_time_flush() is False
        minio.upload_bytes.assert_not_called()

    def test_flushes_when_interval_elapsed(self, processor, minio):
        processor._buffer.append(make_record())
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
        assert isinstance(_to_parquet([make_record()]), bytes)

    def test_schema_has_all_expected_columns(self):
        table = pq.read_table(io.BytesIO(_to_parquet([make_record()])))
        assert set(table.column_names) == {"raw", "route_id", "timestamp_utc", "ingest_ts"}

    def test_route_id_value_matches_record(self):
        rec = make_record("zone3_northern_to_zone6_coastal")
        table = pq.read_table(io.BytesIO(_to_parquet([rec])))
        assert table.column("route_id")[0].as_py() == "zone3_northern_to_zone6_coastal"

    def test_ingest_ts_value_matches_record(self):
        rec = make_record()
        table = pq.read_table(io.BytesIO(_to_parquet([rec])))
        assert table.column("ingest_ts")[0].as_py() == _SAMPLE_INGEST_TS

    def test_raw_stored_as_json_string(self):
        rec = make_record()
        table = pq.read_table(io.BytesIO(_to_parquet([rec])))
        raw_str = table.column("raw")[0].as_py()
        assert isinstance(raw_str, str)
        assert "paths" in json.loads(raw_str)

    def test_multiple_observations_correct_row_count(self):
        batch = [make_record(f"route_{i}") for i in range(5)]
        table = pq.read_table(io.BytesIO(_to_parquet(batch)))
        assert table.num_rows == 5

    def test_timestamp_utc_stored_as_string(self):
        table = pq.read_table(io.BytesIO(_to_parquet([make_record()])))
        ts_type = table.schema.field("timestamp_utc").type
        assert pa.types.is_string(ts_type) or pa.types.is_large_string(ts_type)
