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

### Triết lý thiết kế: Cyclical Time Encoding

Thay vì JOIN với baseline table để tính deviation features, model được train trực tiếp trên joint distribution `(traffic ratios, time-of-day, day-of-week)` thông qua cyclical encoding sin/cos.

**Lý do:**
- Sin/cos encoding khiến 23:00 và 00:00 là hàng xóm trong feature space, Monday và Sunday không bị ngắt quãng qua ranh giới tuần
- IsolationForest học được "heavy traffic lúc 08:00 thứ Hai là bình thường, nhưng lúc 03:00 Chủ nhật là bất thường" — **không cần baseline lookup**
- Loại bỏ dependency vào `gold.traffic_baseline` ở serving time → đơn giản hóa pipeline

### Feature Vector — 7 features theo đúng thứ tự

```python
# ml/features/traffic_features.py
FEATURE_COLUMNS = [
    "avg_heavy_ratio",      # 1 — tỷ lệ xe nặng (0–1)
    "avg_moderate_ratio",   # 2 — tỷ lệ xe trung bình (0–1)
    "max_severe_segments",  # 3 — số đoạn tắc nghẽn nặng (absolute count)
    "hour_sin",             # 4 — sin(2π × hour / 24)
    "hour_cos",             # 5 — cos(2π × hour / 24)
    "dow_sin",              # 6 — sin(2π × dow / 7)   SQL DOW: Sun=0…Sat=6
    "dow_cos",              # 7 — cos(2π × dow / 7)
]
```

### Cách tính từng feature

#### 1–3. Traffic ratios & congestion
```sql
-- Đọc trực tiếp từ gold.traffic_hourly
avg_heavy_ratio                                     -- từ VietMap congestion_heavy_ratio
COALESCE(avg_moderate_ratio, 0.0)
CAST(COALESCE(max_severe_segments, 0) AS DOUBLE)
```

#### 4–7. Cyclical time encoding
```sql
-- DuckDB, tính từ hour_utc của gold row
SIN(2 * PI() * CAST(EXTRACT(HOUR FROM hour_utc) AS DOUBLE) / 24.0) AS hour_sin
COS(2 * PI() * CAST(EXTRACT(HOUR FROM hour_utc) AS DOUBLE) / 24.0) AS hour_cos
SIN(2 * PI() * CAST(EXTRACT(DOW  FROM hour_utc) AS DOUBLE) / 7.0)  AS dow_sin
COS(2 * PI() * CAST(EXTRACT(DOW  FROM hour_utc) AS DOUBLE) / 7.0)  AS dow_cos
```

```python
# Serving layer tái tạo từ window_start (prediction_service.py)
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

**contamination=0.05** → model kỳ vọng 5% data là anomaly khi train.

**Không có baseline.json** — serving layer tự tính cyclical features từ `window_start` timestamp.

### Scoring (Serving, On-demand)

```python
# prediction_service.py — _build_feature_vector(row)
X = np.array([
    mean_heavy_ratio,       # từ online_route_features
    mean_moderate_ratio,    # từ online_route_features (COALESCE 0.0)
    max_severe_segments,    # từ online_route_features (COALESCE 0.0)
    hour_sin,               # tính từ window_start
    hour_cos,
    dow_sin,
    dow_cos,
])
iforest_anomaly = model.predict(X)[0] == -1   # sklearn: -1 = anomaly
iforest_score   = model.decision_function(X)[0]  # âm hơn = bất thường hơn
```

**Model được load lazy per route, TTL = 1h.** Lần đầu gọi `_get_route_cache(route_id)` sẽ fetch từ MLflow (chỉ `model.pkl`).

---

## Dual-Signal Merge

| Signal | Tính khi nào | Threshold | Field |
|--------|-------------|-----------|-------|
| Z-Score | Mỗi Kafka message (online) | `\|z\| > 3.0` | `is_anomaly` |
| IsolationForest | On-demand khi serving query (SSE mỗi 15s) | `predict == -1` | `iforest_anomaly` |

```python
both_anomaly    = is_anomaly AND iforest_anomaly   # most reliable
zscore_only     = is_anomaly AND NOT iforest_anomaly
iforest_only    = NOT is_anomaly AND iforest_anomaly
```

### Majority Vote (route_iforest_scores)

IForest được score mỗi 15s qua SSE. Kết quả được aggregate theo giờ với majority vote:

```sql
-- Anomaly chỉ tính khi >= 50% scoring cycles trong giờ đó đồng ý
anomaly_count::float / score_count >= 0.5   -- iforest_anomaly majority
both_count::float    / score_count >= 0.5   -- both_anomaly majority
```

Bảng `route_iforest_scores` lưu: `(route_id, window_start, score_count, anomaly_count, both_count)`.

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

*Lưu ý: `gold.traffic_baseline` chỉ dùng cho Z-score online. IsolationForest không cần bảng này.*

---

## Known Limitations

**Z-score = NULL khi:**
- Baseline chưa compute (cần `make bootstrap`)
- VietMap API trả về duration không đổi → `stddev ≈ 0` trong window online → zscore vẫn tính được (dùng baseline stddev, không phải window stddev)

**IForest không tin cậy khi:**
- Model được train trên < 10 samples per route (bị skip)
- `champion` alias chưa được set — serving fall back sang `latest` version
- `window_start` NULL trong Postgres row → cyclical features default về `hour=0, dow=0` (Sunday midnight)

**Training/serving consistency:**
- Cyclical features được tính hoàn toàn giống nhau ở cả training (DuckDB SQL) và serving (Python `math.sin/cos`) — không có xấp xỉ
- `max_severe_segments` ở serving là max trong window hiện tại, training là max trong hourly aggregate — tương đương khi window = 1 giờ
- `mean_moderate_ratio` ở serving đọc từ `online_route_features`; nếu NULL thì COALESCE về 0.0 giống training
