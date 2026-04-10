"""Shared fixtures for online service tests."""
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from urbanpulse_core.models.traffic import CongestionMetrics, TrafficRouteObservation


@pytest.fixture
def sample_obs() -> TrafficRouteObservation:
    return TrafficRouteObservation(
        route_id="zone1_urban_core_to_zone4_southern_port",
        origin="Urban Core",
        destination="Southern Port",
        distance_meters=5000.0,
        duration_ms=600000.0,
        duration_minutes=10.0,
        congestion=CongestionMetrics(
            heavy_ratio=0.2,
            moderate_ratio=0.3,
            low_ratio=0.5,
            severe_segments=1,
            total_segments=10,
        ),
        timestamp_utc=datetime(2026, 4, 10, 8, 0, 0, tzinfo=timezone.utc),
    )


def make_kafka_msg(obs: TrafficRouteObservation, ingest_ts_ms: int | None = None) -> MagicMock:
    msg = MagicMock()
    msg.value.return_value = obs.model_dump_json().encode()
    headers = [("ingest_ts", str(ingest_ts_ms).encode())] if ingest_ts_ms else []
    msg.headers.return_value = headers
    return msg
