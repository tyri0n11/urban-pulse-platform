"""Shared fixtures for ml service tests."""
import math

import numpy as np
import pyarrow as pa
import pytest


def make_feature_table(
    n_rows: int = 60,
    heavy_ratios: list[float] | None = None,
    route_id: str = "zone1_test_to_zone4_test",
) -> pa.Table:
    """Build a minimal PyArrow feature table matching FEATURE_COLUMNS schema."""
    rng = np.random.default_rng(seed=42)
    if heavy_ratios is None:
        heavy = rng.uniform(0.0, 0.4, n_rows).tolist()
    else:
        heavy = list(heavy_ratios)
        n_rows = len(heavy)

    moderate = rng.uniform(0.0, 0.3, n_rows).tolist()
    severe = rng.integers(0, 5, n_rows).astype(float).tolist()
    hours = [i % 24 for i in range(n_rows)]
    dows = [i % 7 for i in range(n_rows)]

    return pa.table({
        "route_id": [route_id] * n_rows,
        "hour_utc": pa.array(
            [f"2026-04-{10 + i // 24:02d}T{i % 24:02d}:00:00" for i in range(n_rows)]
        ),
        "avg_heavy_ratio": heavy,
        "avg_moderate_ratio": moderate,
        "max_severe_segments": severe,
        "hour_sin": [math.sin(2 * math.pi * h / 24) for h in hours],
        "hour_cos": [math.cos(2 * math.pi * h / 24) for h in hours],
        "dow_sin": [math.sin(2 * math.pi * d / 7) for d in dows],
        "dow_cos": [math.cos(2 * math.pi * d / 7) for d in dows],
    })


@pytest.fixture
def feature_table() -> pa.Table:
    return make_feature_table()
