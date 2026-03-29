# Urban Pulse — ML & Anomaly Detection

## Z-Score (Online, Real-time)

### Cách tính

Z-score đo lường "số độ lệch chuẩn so với baseline lịch sử":

```
zscore = (current_mean - baseline_mean) / baseline_stddev
```

| Biến | Nguồn | Mô tả |
|------|-------|-------|
| `current_mean` | `online_route_features.mean_duration_minutes` | Welford running mean của cửa sổ UTC hiện tại (reset mỗi giờ) |
| `baseline_mean` | `gold.traffic_baseline.baseline_duration_mean` | Trung bình lịch sử cho route × (day_of_week, hour_of_day) |
| `baseline_stddev` | `gold.traffic_baseline.baseline_duration_stddev` | Độ lệch chuẩn lịch sử; fallback = `0.1 × mean` nếu chỉ có 1 mẫu |

```python
# online/app.py
if baseline and baseline.stddev > 0:
    zscore = (window.mean_duration - baseline.mean) / baseline.stddev
    is_anomaly = abs(zscore) > 3.0
```

**Threshold:** `|zscore| > 3.0` → `is_anomaly = True`

### Khi nào zscore = NULL

- `gold.traffic_baseline` trống (bootstrap chưa chạy)
- Route không có baseline entry cho (day_of_week, hour_of_day) hiện tại
- `baseline.stddev = 0` (bị filter trước khi tính)

---

## Welford's Online Algorithm (Running Mean & Stddev)

Online service dùng Welford's để tính mean/stddev theo từng message mà không cần lưu raw observations:

```python
# online/models.py — RouteWindow.update()
delta  = duration - self.mean_duration
self.mean_duration += delta / self.count          # running mean
delta2 = duration - self.mean_duration
self.M2_duration   += delta * delta2              # sum of squared deviations

# stddev = sqrt(M2 / (n - 1))   [sample stddev, Bessel's correction]
```

**Đặc điểm:**
- Constant memory — không lưu raw values
- Reset khi qua giờ UTC mới (`window_start_ts` thay đổi)
- Cần ≥ 2 observations để có stddev khác 0

---

## Baseline (Batch, 6h)

Tính từ toàn bộ gold data, group by `(route_id, day_of_week, hour_of_day)`:

```sql
-- batch/jobs/baseline_learning.py
SELECT
    route_id,
    EXTRACT(DOW  FROM hour_utc) AS day_of_week,   -- 0=Sun, 6=Sat
    EXTRACT(HOUR FROM hour_utc) AS hour_of_day,
    AVG(avg_duration_minutes)   AS baseline_duration_mean,
    COALESCE(
        STDDEV(avg_duration_minutes),
        AVG(avg_duration_minutes) * 0.1            -- fallback nếu chỉ 1 mẫu
    )                           AS baseline_duration_stddev,
    AVG(avg_heavy_ratio)        AS baseline_heavy_ratio_mean,
    COUNT(*)                    AS sample_count
FROM gold.traffic_hourly
GROUP BY route_id, day_of_week, hour_of_day
```

**Baseline được load vào online service:**
- Khi startup
- Tự refresh mỗi 6h (`_BASELINE_TTL = 6 * 3600`)
- Scope tới `(day_of_week, hour_of_day)` hiện tại → O(1) lookup per message

---

## IsolationForest (Batch, Per-route)

### Feature Vector — 5 features theo đúng thứ tự

```python
FEATURE_COLUMNS = [
    "duration_zscore",       # 1
    "heavy_ratio_deviation", # 2
    "p95_to_mean_ratio",     # 3
    "observation_count",     # 4
    "max_severe_segments",   # 5
]
```

### Cách tính từng feature

#### 1. `duration_zscore`
```
(avg_duration - baseline_mean) / baseline_stddev
```
- **Training:** tính trong `_FEATURE_SQL` (DuckDB JOIN gold × baseline)
- **Serving:** đọc trực tiếp từ `online_route_features.duration_zscore`

#### 2. `heavy_ratio_deviation`
```
avg_heavy_ratio - baseline_heavy_ratio_mean
```
- Đo lường xe nặng bất thường so với baseline cùng giờ/ngày
- **Training:** `g.avg_heavy_ratio - b.baseline_heavy_ratio_mean` (từ gold JOIN baseline)
- **Serving:** `window.mean_heavy_ratio - baseline.heavy_ratio_mean` (online service tính, lưu vào Postgres)
- Fallback nếu không có baseline: dùng raw `mean_heavy_ratio`

#### 3. `p95_to_mean_ratio`
```
p95_duration / avg_duration
```
- Đo độ "spike" — ratio cao = có outlier outliers trong giờ đó
- **Training:** `g.p95_duration_minutes / g.avg_duration_minutes` (true p95 từ gold hourly aggregate)
- **Serving:** `(mean + 2σ) / mean` ≈ 97.7th percentile dưới normal distribution
- Giá trị = 1.0 nếu mean = 0 hoặc stddev = 0

#### 4. `observation_count`
```
COUNT(*) của window
```
- Raw count, cast to DOUBLE
- **Training:** `gold.traffic_hourly.observation_count`
- **Serving:** `online_route_features.observation_count`

#### 5. `max_severe_segments`
```
MAX(congestion.severe_segments) trong window
```
- Đoạn đường nặng nhất trong giờ đó
- **Training:** `gold.traffic_hourly.max_severe_segments` (DuckDB `MAX()` của silver)
- **Serving:** `RouteWindow.max_severe_segments` (tracked per-message, stored in Postgres)

### Training pipeline

```
gold.traffic_hourly  ──┐
                        ├─► DuckDB JOIN → feature table → IsolationForest.fit()
gold.traffic_baseline ──┘                                       ↓
                                                     MLflow: iforest-{route_id}@champion
```

```python
# train.py — per route, needs >= 10 samples
if features.num_rows < 10:
    return {"status": "skipped"}

iforest = IsolationForest(contamination=0.05, n_estimators=100, random_state=42)
iforest.fit(features)   # X shape: (n_samples, 5)
```

**contamination=0.05** → model kỳ vọng 5% data là anomaly khi train.

### Scoring (Serving, On-demand)

```python
# prediction_service.py
X = np.array([
    duration_zscore,        # từ Postgres
    heavy_ratio_deviation,  # từ Postgres
    p95_to_mean_ratio,      # từ Postgres
    observation_count,      # từ Postgres
    max_severe_segments,    # từ Postgres
])
iforest_anomaly = model.predict(X)[0] == -1   # sklearn: -1 = anomaly
iforest_score   = model.decision_function(X)[0]  # âm hơn = bất thường hơn
```

**Model được load lazy per route, TTL = 1h.** Lần đầu gọi `_get_route_model(route_id)` sẽ fetch từ MLflow.

---

## Dual-Signal Merge

| Signal | Tính khi nào | Threshold | Field |
|--------|-------------|-----------|-------|
| Z-Score | Mỗi Kafka message (online) | `\|z\| > 3.0` | `is_anomaly` |
| IsolationForest | On-demand khi serving query | `predict == -1` | `iforest_anomaly` |

```python
both_anomaly    = is_anomaly AND iforest_anomaly   # most reliable
zscore_only     = is_anomaly AND NOT iforest_anomaly
iforest_only    = NOT is_anomaly AND iforest_anomaly
```

---

## Gold Layer Schema

```
gold.traffic_hourly      — aggregated per (route_id, hour_utc)
    route_id, origin, destination
    hour_utc              TIMESTAMPTZ   UTC hour bucket
    observation_count     INT
    avg_duration_minutes  DOUBLE        AVG(duration_minutes) trong giờ
    p95_duration_minutes  DOUBLE        PERCENTILE_CONT(0.95)
    avg_heavy_ratio       DOUBLE        AVG(congestion_heavy_ratio)
    avg_moderate_ratio    DOUBLE        AVG(congestion_moderate_ratio)
    max_severe_segments   INT           MAX(congestion_severe_segments)

gold.traffic_baseline    — historical stats per (route_id, day_of_week, hour_of_day)
    route_id
    day_of_week           INT           0=Sun, 1=Mon … 6=Sat
    hour_of_day           INT           0–23 (UTC)
    baseline_duration_mean    DOUBLE
    baseline_duration_stddev  DOUBLE    fallback = mean * 0.1 nếu 1 mẫu
    baseline_heavy_ratio_mean DOUBLE
    sample_count          INT
```

---

## Known Limitations

**Z-score = NULL khi:**
- Baseline chưa compute (cần `make bootstrap`)
- VietMap API trả về duration không đổi → `stddev ≈ 0` trong window online → zscore vẫn tính được (dùng baseline stddev, không phải window stddev)

**IForest không tin cậy khi:**
- Model được train trên < 10 samples per route (bị skip)
- `champion` alias chưa được set — serving fall back sang `latest` version
- Features bị NULL → `nan_to_num(nan=0.0)` làm nhiễu

**Training/serving consistency:**
- `p95_to_mean_ratio` ở serving là xấp xỉ (`mean + 2σ`) thay vì true p95 — đủ gần dưới normal distribution
- `max_severe_segments` ở serving là max trong window hiện tại, training là max trong hourly aggregate — tương đương khi window = 1 giờ
