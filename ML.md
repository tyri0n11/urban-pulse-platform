# Urban Pulse — ML & Anomaly Detection

## Z-Score (Online, Real-time)

### How it's computed

Z-score measures "number of standard deviations from the historical baseline":

```
zscore = (current_mean - baseline_mean) / baseline_stddev
```

| Variable | Source | Description |
|----------|--------|-------------|
| `current_mean` | `online_route_features.mean_duration_minutes` | Welford running mean for the current UTC window (resets every hour) |
| `baseline_mean` | `gold.traffic_baseline.baseline_duration_mean` | Historical mean for route × (day_of_week, hour_of_day) |
| `baseline_stddev` | `gold.traffic_baseline.baseline_duration_stddev` | Historical stddev; fallback = `0.1 × mean` if only 1 sample |

```python
# online/app.py
if baseline and baseline.stddev > 0:
    zscore = (window.mean_duration - baseline.mean) / baseline.stddev
    is_anomaly = zscore > 3.0   # one-sided: only high duration (congestion) is flagged
```

**Threshold:** `zscore > 3.0` → `is_anomaly = True` (one-sided — low duration / fast traffic is not an anomaly)

### When zscore is NULL

- `gold.traffic_baseline` is empty (bootstrap hasn't run yet)
- No baseline entry for the current (day_of_week, hour_of_day) slot
- `baseline.stddev = 0` (filtered before computation)

---

## Welford's Online Algorithm (Running Mean & Stddev)

The online service uses Welford's algorithm to compute mean/stddev per message without storing raw observations:

```python
# online/models.py — RouteWindow.update()
delta  = duration - self.mean_duration
self.mean_duration += delta / self.count          # running mean
delta2 = duration - self.mean_duration
self.M2_duration   += delta * delta2              # sum of squared deviations

# stddev = sqrt(M2 / (n - 1))   [sample stddev, Bessel's correction]
```

**Properties:**
- Constant memory — no raw values stored
- Resets when a new UTC hour begins (`window_start_ts` changes)
- Requires ≥ 2 observations to produce stddev > 0

---

## Baseline (Batch, 6h)

Computed from all gold data, grouped by `(route_id, day_of_week, hour_of_day)`:

```sql
-- batch/jobs/baseline_learning.py
SELECT
    route_id,
    EXTRACT(DOW  FROM hour_utc) AS day_of_week,   -- 0=Sun, 6=Sat
    EXTRACT(HOUR FROM hour_utc) AS hour_of_day,
    AVG(avg_duration_minutes)   AS baseline_duration_mean,
    COALESCE(
        STDDEV(avg_duration_minutes),
        AVG(avg_duration_minutes) * 0.1            -- fallback if only 1 sample
    )                           AS baseline_duration_stddev,
    AVG(avg_heavy_ratio)        AS baseline_heavy_ratio_mean,
    COUNT(*)                    AS sample_count
FROM gold.traffic_hourly
GROUP BY route_id, day_of_week, hour_of_day
```

**Baseline is loaded into the online service:**
- At startup
- Automatically refreshed every 6h (`_BASELINE_TTL = 6 * 3600`)
- Scoped to the current `(day_of_week, hour_of_day)` → O(1) lookup per message

---

## IsolationForest (Batch, Per-route)

### Design philosophy: Cyclical Time Encoding

Instead of joining with the baseline table to compute deviation features, the model is trained directly on the joint distribution of `(traffic ratios, time-of-day, day-of-week)` using sin/cos cyclical encoding.

**Why:**
- Sin/cos encoding makes 23:00 and 00:00 neighbors in feature space; Monday and Sunday are not severed across a week boundary
- IsolationForest learns "heavy traffic at 08:00 Monday is normal, but at 03:00 Sunday is anomalous" — **no baseline lookup needed**
- Removes the dependency on `gold.traffic_baseline` at serving time → simpler pipeline

### Feature Vector — 7 features in exact order

```python
# ml/features/traffic_features.py
FEATURE_COLUMNS = [
    "avg_heavy_ratio",      # 1 — heavy vehicle ratio (0–1)
    "avg_moderate_ratio",   # 2 — moderate congestion ratio (0–1)
    "max_severe_segments",  # 3 — count of severely congested segments
    "hour_sin",             # 4 — sin(2π × hour / 24)
    "hour_cos",             # 5 — cos(2π × hour / 24)
    "dow_sin",              # 6 — sin(2π × dow / 7)   SQL DOW: Sun=0…Sat=6
    "dow_cos",              # 7 — cos(2π × dow / 7)
]
```

### How each feature is computed

#### 1–3. Traffic ratios & congestion
```sql
-- Read directly from gold.traffic_hourly
avg_heavy_ratio                                     -- from VietMap congestion_heavy_ratio
COALESCE(avg_moderate_ratio, 0.0)
CAST(COALESCE(max_severe_segments, 0) AS DOUBLE)
```

#### 4–7. Cyclical time encoding
```sql
-- DuckDB, computed from hour_utc of gold row
SIN(2 * PI() * CAST(EXTRACT(HOUR FROM hour_utc) AS DOUBLE) / 24.0) AS hour_sin
COS(2 * PI() * CAST(EXTRACT(HOUR FROM hour_utc) AS DOUBLE) / 24.0) AS hour_cos
SIN(2 * PI() * CAST(EXTRACT(DOW  FROM hour_utc) AS DOUBLE) / 7.0)  AS dow_sin
COS(2 * PI() * CAST(EXTRACT(DOW  FROM hour_utc) AS DOUBLE) / 7.0)  AS dow_cos
```

```python
# Serving layer reconstructs from window_start (prediction_service.py)
hour = window_start.astimezone(utc).hour
dow  = (window_start.weekday() + 1) % 7   # Python weekday → SQL DOW (Sun=0)
hour_sin = math.sin(2 * math.pi * hour / 24)
hour_cos = math.cos(2 * math.pi * hour / 24)
dow_sin  = math.sin(2 * math.pi * dow  / 7)
dow_cos  = math.cos(2 * math.pi * dow  / 7)
```

### Training pipeline

```
gold.traffic_hourly
    └─► build_features() → 7-feature Arrow table
            └─► IsolationForest.fit()   X shape: (n_samples, 7)
                    └─► MLflow: iforest-{route_id} (model.pkl only)
```

```python
# train.py — per route, needs >= 10 samples
if features.num_rows < 10:
    return {"status": "skipped"}

iforest = IsolationForest(contamination=0.05, n_estimators=100, random_state=42)
iforest.fit(X)   # X shape: (n_samples, 7)
```

**contamination=0.05** → model expects 5% of training data to be anomalous.

**No baseline.json** — the serving layer computes cyclical features from the `window_start` timestamp directly.

### Scoring (Serving, On-demand)

```python
# prediction_service.py — _build_feature_vector(row)
X = np.array([
    mean_heavy_ratio,       # from online_route_features
    mean_moderate_ratio,    # from online_route_features (COALESCE 0.0)
    max_severe_segments,    # from online_route_features (COALESCE 0.0)
    hour_sin,               # computed from window_start
    hour_cos,
    dow_sin,
    dow_cos,
])
iforest_anomaly = model.predict(X)[0] == -1   # sklearn: -1 = anomaly
iforest_score   = model.decision_function(X)[0]  # more negative = more anomalous
```

**Model is loaded lazily per route with TTL = 1h.** The first call to `_get_route_cache(route_id)` fetches from MLflow (`model.pkl` only).

---

## Dual-Signal Merge

| Signal | Computed | Threshold | Field |
|--------|----------|-----------|-------|
| Z-Score | Every Kafka message (online) | `\|z\| > 3.0` | `is_anomaly` |
| IsolationForest | On-demand when serving queries (SSE every 15s) | `predict == -1` | `iforest_anomaly` |

```python
both_anomaly    = is_anomaly AND iforest_anomaly   # most reliable
zscore_only     = is_anomaly AND NOT iforest_anomaly
iforest_only    = NOT is_anomaly AND iforest_anomaly
```

### Majority Vote (route_iforest_scores)

IForest is scored every 15s via SSE. Results are aggregated per hour with majority vote:

```sql
-- Anomaly counted only when >= 50% of scoring cycles in that hour agree
anomaly_count::float / score_count >= 0.5   -- iforest_anomaly majority
both_count::float    / score_count >= 0.5   -- both_anomaly majority
```

`route_iforest_scores` stores: `(route_id, window_start, score_count, anomaly_count, both_count)`.

---

## Gold Layer Schema

```
gold.traffic_hourly      — aggregated per (route_id, hour_utc)
    route_id, origin, destination
    hour_utc              TIMESTAMPTZ   UTC hour bucket
    observation_count     INT
    avg_duration_minutes  DOUBLE        AVG(duration_minutes) within the hour
    p95_duration_minutes  DOUBLE        PERCENTILE_CONT(0.95)
    avg_heavy_ratio       DOUBLE        AVG(congestion_heavy_ratio)
    avg_moderate_ratio    DOUBLE        AVG(congestion_moderate_ratio)
    max_severe_segments   INT           MAX(congestion_severe_segments)

gold.traffic_baseline    — historical stats per (route_id, day_of_week, hour_of_day)
    route_id
    day_of_week           INT           0=Sun, 1=Mon … 6=Sat
    hour_of_day           INT           0–23 (UTC)
    baseline_duration_mean    DOUBLE
    baseline_duration_stddev  DOUBLE    fallback = mean * 0.1 if only 1 sample
    baseline_heavy_ratio_mean DOUBLE
    sample_count          INT
```

*Note: `gold.traffic_baseline` is only used for Z-score online. IsolationForest does not require this table.*

---

## Known Limitations

**Z-score is NULL when:**
- Baseline not yet computed (run `make bootstrap`)
- VietMap API returns constant duration → `stddev ≈ 0` in the online window → zscore can still be computed (uses baseline stddev, not window stddev)

**IForest is unreliable when:**
- Model trained on < 10 samples per route (skipped)
- `champion` alias not yet set — serving falls back to `latest` version
- `window_start` is NULL in Postgres row → cyclical features default to `hour=0, dow=0` (Sunday midnight)

**Training/serving consistency:**
- Cyclical features are computed identically in both training (DuckDB SQL) and serving (Python `math.sin/cos`) — no approximation
- `max_severe_segments` in serving is max within the current window; in training it's max within the hourly aggregate — equivalent when window = 1 hour
- `mean_moderate_ratio` in serving reads from `online_route_features`; if NULL it COALESCEs to 0.0 matching training

---

## End-to-End Latency & SLOs

Each scoring cycle in `_iforest_scorer_loop` (every 15s) writes a row to `prediction_history` with a full latency breakdown:

```
ingest_lag_ms  = time(Postgres write) - ingest_ts header    # pipeline input cost
staleness_ms   = time(IForest score)  - updated_at          # data age at scoring time
scoring_ms     = IForest predict() wall clock
full_e2e_ms    = ingest_lag_ms + staleness_ms + scoring_ms
```

### SLO 1 — Pipeline Processing Latency

**Measured by:** `ingest_lag_ms + scoring_ms` — system overhead only, excluding poll wait time.

**Target:** p95 < 60 s — the portion the system controls (Kafka + online service).

**Typical values:** ingest_lag_ms ~10–30s (Kafka + online service), scoring_ms ~50–500ms (IForest predict).

### SLO 2 — Data Freshness

**Measured by:** `staleness_ms` — age of data at scoring time.

**Target:** p95 < 310 s — VietMap poll interval 5 min + 10s buffer.

**Typical values:** staleness_ms usually 0–310s, dominated by VietMap poll frequency (external constraint), not a system fault.

### Why two separate SLOs

Full e2e > 5 minutes is expected and correct when staleness dominates — it reflects VietMap API poll frequency, not a system failure. Reporting them separately allows honest thesis evaluation and enables targeted corrective action.
