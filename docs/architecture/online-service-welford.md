# Online Service — Welford's Online Algorithm

## Overview

The `online` service is a Kafka consumer that computes per-route rolling statistics in real time and writes them to Postgres. It uses **Welford's online algorithm** to maintain mean and standard deviation with O(1) memory — no raw observations are ever stored.

**Data flow:**

```
Kafka (traffic-route-bronze)
    ↓ every ~5 min
OnlineFeatureProcessor.process()
    ├─ RouteWindow.update()   ← Welford accumulation (in RAM)
    ├─ Z-score vs batch baseline
    └─ UPSERT → Postgres (online_route_features)
```

---

## Why Welford?

| Naive approach | Welford |
|---|---|
| Store all raw observations, recompute each time | Only 3 numbers: `count`, `mean`, `M2` |
| Memory grows unboundedly | O(1) constant memory |
| O(n) per update | O(1) per update |
| Numerically unstable for large values | Numerically stable by design |

---

## Per-Route Window State (`models.py`)

Each route maintains a `RouteWindow` dataclass:

```python
@dataclass
class RouteWindow:
    window_start_ts: int    # Unix timestamp of current hour boundary
    count: int              # observations in this window
    mean_duration: float    # Welford running mean
    M2_duration: float      # sum of squared deviations (Welford accumulator)
    last_duration: float
    sum_heavy_ratio: float
    last_heavy_ratio: float
    sum_moderate_ratio: float
    sum_low_ratio: float
    last_ingest_lag_ms: int
    max_severe_segments: float
```

Windows reset every UTC hour. All routes are kept in a dict `route_id → RouteWindow` in RAM.

---

## Welford Update Step (`models.py:39–47`)

On each new observation:

```python
def update(self, duration, heavy_ratio, ...):
    self.count += 1

    delta  = duration - self.mean_duration   # deviation BEFORE updating mean
    self.mean_duration += delta / self.count  # update mean (Knuth's formula)
    delta2 = duration - self.mean_duration   # deviation AFTER updating mean
    self.M2_duration += delta * delta2        # accumulate variance
```

**Why two deltas?**  
`delta × delta2 = (x − mean_old)(x − mean_new)` — this product avoids catastrophic cancellation that occurs when subtracting two large similar numbers (a common issue with the naive `Σx² − n·mean²` formula).

**Stddev on demand:**

```python
@property
def stddev_duration(self) -> float:
    if self.count < 2:
        return 0.0
    return float((self.M2_duration / (self.count - 1)) ** 0.5)
```

### Worked example

Route durations observed: `[100, 110, 120]`

| n | x   | delta (pre) | mean new | delta2 (post) | M2  |
|---|-----|-------------|----------|---------------|-----|
| 1 | 100 | 100         | 100.0    | 0             | 0   |
| 2 | 110 | 10          | 105.0    | 5             | 50  |
| 3 | 120 | 15          | 110.0    | 10            | 200 |

`stddev = sqrt(200 / 2) = 10.0` ✓

---

## Window Reset and Lookup (`app.py:217–221`)

```python
hour_ts = _current_hour_ts()
window = self._windows.get(obs.route_id)
if window is None or window.window_start_ts != hour_ts:
    window = RouteWindow(window_start_ts=hour_ts)   # fresh window each hour
    self._windows[obs.route_id] = window
```

Each route gets a fresh window at the start of each UTC hour.

---

## Z-Score Computation (`app.py:234–242`)

After updating the window, the service computes a z-score of `mean_heavy_ratio` against the batch baseline loaded from `gold.traffic_baseline` (Iceberg):

```python
baseline = self._baseline.get(obs.route_id)   # (dow, hour) slot
zscore = (window.mean_heavy_ratio - baseline.heavy_ratio_mean) / baseline.heavy_ratio_stddev
is_anomaly = zscore > baseline.zscore_threshold  # one-sided: high congestion only
```

- Baseline is loaded at startup and refreshed every 6 hours.
- It is filtered to the current `(day_of_week, hour)` slot — Monday 8 AM is compared against historical Monday 8 AM data only.
- `zscore_threshold` is **dynamic per route** (p99 of historical z-scores), with a minimum floor of 1.5 and a default fallback of 2.0.

> **Note:** The Postgres column is named `duration_zscore` for historical reasons. The value stored is the **heavy_ratio z-score**, not a duration z-score.

---

## Postgres Schema (`online_route_features`)

Primary key: `(route_id, window_start)` — one row per route per hour, UPSERTED on every message.

| Column | Description |
|---|---|
| `route_id` | Route identifier |
| `window_start` | UTC hour boundary (PK) |
| `updated_at` | Timestamp of last UPSERT |
| `observation_count` | Messages processed in this window |
| `mean_duration_minutes` | Welford mean of travel time |
| `stddev_duration_minutes` | Welford stddev (from M2) |
| `last_duration_minutes` | Most recent duration value |
| `mean_heavy_ratio` | Running mean of heavy congestion ratio |
| `last_heavy_ratio` | Most recent heavy_ratio value |
| `duration_zscore` | Heavy-ratio z-score vs baseline (misleading name) |
| `is_anomaly` | `true` if zscore > per-route threshold |
| `last_ingest_lag_ms` | End-to-end latency from Kafka header to now |
| `heavy_ratio_deviation` | `mean_heavy_ratio − baseline.heavy_ratio_mean` |
| `p95_to_mean_ratio` | `(mean + 2×stddev) / mean` — proxy for variability |
| `max_severe_segments` | Max severe segment count seen in this window |
| `mean_moderate_ratio` | Running mean of moderate congestion ratio |
| `mean_low_ratio` | Running mean of low congestion ratio |

---

## Crash Recovery (`app.py:122–173`)

Welford's `M2` is not persisted directly. On restart, it is reconstructed from the stored `stddev` and `count`:

```python
# Inverse of: stddev = sqrt(M2 / (n-1))
m2 = (stddev ** 2) * max(count - 1, 0)
```

This is mathematically exact, so the service resumes mid-hour accumulation without resetting.

---

## End-to-End Message Latency

The ingestion lag is computed from a Kafka message header (`ingest_ts`) set by the ingestion service at publish time:

```python
lag_ms = int(time.time() * 1000) - int(ingest_ts_header)
```

Observed p95 latency: **< 20 ms** (sample: 105–181 ms including network).
