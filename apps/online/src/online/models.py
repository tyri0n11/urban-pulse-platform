"""Per-route windowed state for online feature computation."""

from dataclasses import dataclass, field


@dataclass
class RouteWindow:
    """Rolling stats for a single route within the current hourly window.

    Statistics are maintained using Welford's online algorithm so we never
    need to store raw observations — constant memory regardless of volume.
    Stats reset when the wall-clock crosses into a new UTC hour.
    """

    window_start_ts: int = 0  # Unix timestamp of the current hour boundary

    # Observation count for the current window
    count: int = 0

    # Welford's accumulators for duration_minutes
    mean_duration: float = 0.0
    M2_duration: float = 0.0   # sum of squared deviations — used for stddev

    # Last observed value (useful for "current state" queries)
    last_duration: float = 0.0

    # Heavy vehicle ratio (simple running sum — cheap to track)
    sum_heavy_ratio: float = 0.0
    last_heavy_ratio: float = 0.0

    # End-to-end ingestion lag for the most recent message
    last_ingest_lag_ms: int = 0

    def update(self, duration: float, heavy_ratio: float, ingest_lag_ms: int) -> None:
        """Apply one observation, updating all accumulators in-place."""
        self.count += 1

        # Welford's step for duration
        delta = duration - self.mean_duration
        self.mean_duration += delta / self.count
        delta2 = duration - self.mean_duration
        self.M2_duration += delta * delta2

        self.last_duration = duration
        self.sum_heavy_ratio += heavy_ratio
        self.last_heavy_ratio = heavy_ratio
        self.last_ingest_lag_ms = ingest_lag_ms

    @property
    def stddev_duration(self) -> float:
        if self.count < 2:
            return 0.0
        return (self.M2_duration / (self.count - 1)) ** 0.5

    @property
    def mean_heavy_ratio(self) -> float:
        if self.count == 0:
            return 0.0
        return self.sum_heavy_ratio / self.count

    def to_dict(self) -> dict[str, object]:
        return {
            "window_start_ts": self.window_start_ts,
            "count": self.count,
            "mean_duration": self.mean_duration,
            "M2_duration": self.M2_duration,
            "last_duration": self.last_duration,
            "sum_heavy_ratio": self.sum_heavy_ratio,
            "last_heavy_ratio": self.last_heavy_ratio,
            "last_ingest_lag_ms": self.last_ingest_lag_ms,
        }

    @classmethod
    def from_dict(cls, d: dict[str, object]) -> "RouteWindow":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})  # type: ignore[attr-defined]
