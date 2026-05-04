"""Shared pytest fixtures for traffic-ingestion tests."""
from datetime import datetime, timezone
from typing import Any

import pytest

_SAMPLE_RAW_RESPONSE: dict[str, Any] = {
    "paths": [{
        "distance": 12500.0,
        "time": 900000.0,
        "annotations": {
            "congestion": [
                {"value": "heavy"}, {"value": "heavy"},
                {"value": "moderate"}, {"value": "moderate"}, {"value": "moderate"},
                {"value": "low"}, {"value": "low"}, {"value": "low"},
                {"value": "low"}, {"value": "low"},
            ]
        },
    }]
}


@pytest.fixture
def sample_raw_response() -> dict[str, Any]:
    return _SAMPLE_RAW_RESPONSE


@pytest.fixture
def sample_route_id() -> str:
    return "zone1_urban_core_to_zone4_southern_port"


@pytest.fixture
def sample_polled_at_ms() -> int:
    return 999888777


@pytest.fixture
def sample_timestamp_utc() -> datetime:
    return datetime(2026, 4, 10, 8, 0, 0, tzinfo=timezone.utc)


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
