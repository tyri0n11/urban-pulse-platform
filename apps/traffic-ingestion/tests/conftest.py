"""Shared pytest fixtures for traffic-ingestion tests."""
from datetime import datetime, timezone

import pytest

from urbanpulse_core.models.traffic import CongestionMetrics, TrafficRouteObservation


@pytest.fixture
def sample_observation() -> TrafficRouteObservation:
    return TrafficRouteObservation(
        route_id="zone1_urban_core_to_zone4_southern_port",
        origin="Urban Core",
        destination="Southern Port",
        distance_meters=12500.0,
        duration_ms=900000.0,
        duration_minutes=15.0,
        congestion=CongestionMetrics(
            heavy_ratio=0.2,
            moderate_ratio=0.3,
            low_ratio=0.5,
            severe_segments=2,
            total_segments=10,
        ),
        timestamp_utc=datetime(2026, 4, 10, 8, 0, 0, tzinfo=timezone.utc),
    )


@pytest.fixture
def sample_routes() -> list[dict]:
    return [
        {
            "route_id": "zone1_urban_core_to_zone4_southern_port",
            "origin": "Urban Core",
            "destination": "Southern Port",
            "origin_anchor": [10.78, 106.70],
            "destination_anchor": [10.73, 106.72],
        },
        {
            "route_id": "zone2_eastern_innovation_to_zone3_northern_industrial",
            "origin": "Eastern Innovation",
            "destination": "Northern Industrial",
            "origin_anchor": [10.80, 106.75],
            "destination_anchor": [10.85, 106.65],
        },
    ]
