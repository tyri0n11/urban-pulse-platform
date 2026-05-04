"""Shared fixtures for online service tests."""
import json
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
    """Build a mock Kafka message matching the new wire format.

    Body  = raw VietMap API JSON (paths + congestion segments).
    Headers = route_id, timestamp_utc, and optionally ingest_ts.
    """
    cong = obs.congestion
    segs: list[dict[str, str]] = []
    if cong and cong.total_segments > 0:
        total = cong.total_segments
        heavy_n = round(cong.heavy_ratio * total)
        moderate_n = round(cong.moderate_ratio * total)
        low_n = round(cong.low_ratio * total)
        segs = (
            [{"value": "heavy"}] * heavy_n
            + [{"value": "moderate"}] * moderate_n
            + [{"value": "low"}] * low_n
        )

    body = json.dumps({
        "paths": [{
            "time": obs.duration_ms,
            "annotations": {"congestion": segs},
        }]
    }).encode()

    headers: list[tuple[str, bytes]] = [
        ("route_id", obs.route_id.encode()),
        ("timestamp_utc", obs.timestamp_utc.isoformat().encode()),
    ]
    if ingest_ts_ms is not None:
        headers.append(("ingest_ts", str(ingest_ts_ms).encode()))

    msg = MagicMock()
    msg.value.return_value = body
    msg.headers.return_value = headers
    return msg
