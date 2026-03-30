# Urban Pulse — Anomaly Detection Design

Hệ thống phát hiện bất thường dùng **dual-signal**: hai tầng độc lập với bài toán, dữ liệu, và tốc độ khác nhau. Kết quả chỉ đáng tin nhất khi cả hai đồng ý.

---

## Tổng quan: Online vs Offline

| Tiêu chí | Online (Z-Score) | Offline (IsolationForest) |
|----------|-----------------|--------------------------|
| **Câu hỏi** | "Route này ngay lúc này có bất thường không?" | "Tổ hợp features của route này có khác bình thường không?" |
| **Tính khi nào** | Mỗi Kafka message (~5 phút/lần) | On-demand khi serving query |
| **Nguồn dữ liệu** | Welford window hiện tại + gold baseline | 5 features từ Postgres |
| **Chiều dữ liệu** | 1 chiều (duration) | 5 chiều đồng thời |
| **Điểm mạnh** | Real-time, interpretable | Bắt pattern phức tạp, nhiều chiều |
| **Điểm yếu** | Bỏ sót pattern phức tạp | Chậm hơn, không có baseline rõ ràng |
| **Lưu ở đâu** | `online_route_features.is_anomaly` | Computed on-demand, không lưu |
| **Latency** | < 20s từ ingestion | ~100ms per route khi query |

---

## Signal 1: Z-Score (Speed Layer)

### Ý nghĩa

Z-score đo lường **số độ lệch chuẩn** so với lịch sử cùng thời điểm (giờ + ngày trong tuần):

```
zscore = (mean_duration_hiện_tại - baseline_mean) / baseline_stddev
```

Ví dụ: Thứ 2 lúc 15h, route A thường mất 68 phút (±10 phút). Hôm nay 15h route A mất 95 phút → zscore = (95 - 68) / 10 = **+2.7** — gần ngưỡng cảnh báo.

### Pipeline tính toán

```
Kafka message (duration_minutes)
  └─► RouteWindow.update()          # Welford: cập nhật mean, M2
        └─► zscore = (mean - baseline.mean) / baseline.stddev
              └─► is_anomaly = abs(zscore) > baseline.zscore_threshold
                    └─► UPSERT → online_route_features
```

**Dynamic threshold** (`baseline.zscore_threshold`): Thay vì hardcode `3.0`, threshold được tính từ phân phối z-score lịch sử của từng route. Mỗi lần `retrain` flow chạy, `baseline_learning.py` tính `p99` của `|zscore|` trên toàn bộ gold data per-route, lưu vào `gold.traffic_baseline.zscore_p99`. Online service đọc giá trị này khi load baseline. Minimum floor: `1.5`.

Ví dụ thực tế (64 ngày data, 20 routes):
- p90 = 0.771σ  · p95 = 1.100σ  · p97 = 1.350σ  · **p99 = 2.026σ**  · max = 2.475σ

### Welford Online Algorithm

Tính mean và stddev mà không cần lưu raw observations (constant memory):

```python
# Mỗi observation mới:
delta  = x - mean
mean  += delta / count
delta2 = x - mean
M2    += delta * delta2

# Stddev khi cần:
stddev = sqrt(M2 / (count - 1))   # Bessel's correction
```

- Reset khi qua giờ UTC mới (`window_start_ts` thay đổi)
- Cần ≥ 2 observations để có `stddev > 0`
- Restart-safe: `M2` được reconstruct từ Postgres khi startup (`M2 = stddev² × (count-1)`)

### Baseline (gold.traffic_baseline)

Tính từ toàn bộ gold data, group by `(route_id, day_of_week, hour_of_day)`:

```sql
SELECT
    route_id,
    EXTRACT(DOW  FROM hour_utc) AS day_of_week,   -- 0=Sun … 6=Sat
    EXTRACT(HOUR FROM hour_utc) AS hour_of_day,
    AVG(avg_duration_minutes)   AS baseline_duration_mean,
    GREATEST(
        COALESCE(STDDEV(avg_duration_minutes), AVG(avg_duration_minutes) * 0.1),
        1.0                                        -- floor: tránh extreme zscore khi data thưa
    )                           AS baseline_duration_stddev,
    AVG(avg_heavy_ratio)        AS baseline_heavy_ratio_mean
FROM gold.traffic_hourly
GROUP BY route_id, day_of_week, hour_of_day
```

**Load cycle:** Startup + tự refresh mỗi 6h. Chỉ load đúng slot `(dow, hour)` hiện tại → O(1) lookup per message.

### Khi nào zscore = NULL

- `gold.traffic_baseline` chưa compute → chạy `make bootstrap`
- Route không có baseline cho slot hiện tại (chưa đủ dữ liệu lịch sử)
- `baseline.stddev = 0` (không thể xảy ra sau khi có floor `GREATEST(..., 1.0)`)

---

## Signal 2: IsolationForest (Batch Layer)

### Ý nghĩa

IForest học **"vùng bình thường"** trong không gian 5 chiều từ lịch sử nhiều tuần. Điểm nằm ngoài vùng đó là anomaly — kể cả khi từng feature riêng lẻ không vượt ngưỡng.

Ví dụ z-score bỏ sót nhưng IForest phát hiện:
- Duration bình thường nhưng `heavy_ratio` cao bất thường
- Duration tăng nhẹ nhưng đồng thời `p95_to_mean_ratio` cao + `max_severe_segments` cao

### Feature Vector (thứ tự cố định)

```python
FEATURE_COLUMNS = [
    "duration_zscore",        # 1 — context vs lịch sử
    "heavy_ratio_deviation",  # 2 — xe nặng bất thường
    "p95_to_mean_ratio",      # 3 — mức độ spike
    "observation_count",      # 4 — độ tin cậy của window
    "max_severe_segments",    # 5 — đoạn nặng nhất
]
```

#### Cách tính từng feature

| Feature | Training (gold) | Serving (Postgres) |
|---------|----------------|-------------------|
| `duration_zscore` | `(avg_duration - baseline_mean) / baseline_stddev` | `online_route_features.duration_zscore` |
| `heavy_ratio_deviation` | `avg_heavy_ratio - baseline_heavy_ratio_mean` | `online_route_features.heavy_ratio_deviation` |
| `p95_to_mean_ratio` | `p95_duration / avg_duration` (true p95) | `(mean + 2σ) / mean` (normal approx) |
| `observation_count` | `COUNT(*)` từ silver | `online_route_features.observation_count` |
| `max_severe_segments` | `MAX(severe_segments)` từ silver | `online_route_features.max_severe_segments` |

> `p95_to_mean_ratio` ở serving là xấp xỉ (mean + 2σ = 97.7th pct dưới normal distribution). Đủ gần với true p95 cho mục đích anomaly detection.

### Training Pipeline

```
gold.traffic_hourly  ──┐
                        ├─► DuckDB JOIN → feature matrix → IsolationForest.fit()
gold.traffic_baseline ──┘                                        ↓
                                                    MLflow: iforest-{route_id}@champion
```

- Train per-route, cần ≥ 10 samples
- `contamination=0.05` — model kỳ vọng 5% data là anomaly
- Trigger: Prefect `retrain` flow mỗi 6h → `POST ml-service:8000/train`

### Scoring (On-demand)

```python
X = np.array([[zscore, heavy_ratio_dev, p95_ratio, obs_count, max_severe]])
iforest_anomaly = model.predict(X)[0] == -1     # sklearn: -1 = anomaly
iforest_score   = model.decision_function(X)[0] # âm hơn = bất thường hơn
```

Model được load lazy per-route, TTL = 1h. Fallback về zscore-only nếu không có model.

---

## Dual-Signal Merge

```python
# serving/routers/anomalies.py
zscore_anomaly  = row["is_anomaly"]              # từ online_route_features
iforest_anomaly = pred.iforest_anomaly           # IForest score on-demand
both_anomaly    = zscore_anomaly and iforest_anomaly
```

### Ý nghĩa từng kết hợp

| zscore | iforest | Signal | Ý nghĩa | Hành động |
|--------|---------|--------|---------|-----------|
| ✅ | ✅ | `BOTH` | Cả hai đồng ý — cảnh báo tin cậy nhất | Alert ngay |
| ✅ | ❌ | `ZSCORE` | Duration tăng mạnh, pattern chưa lạ theo ML | Theo dõi |
| ❌ | ✅ | `IFOREST` | Pattern lạ nhiều chiều, duration chưa vượt ngưỡng | Theo dõi |
| ❌ | ❌ | — | Bình thường | — |

### Severity mapping (cho notification)

```
both_anomaly    → "high"    — Telegram alert
zscore_anomaly  → "medium"  — log + dashboard
iforest_anomaly → "low"     — log + dashboard
```

---

## Known Issues & Mitigations

### Extreme z-score (+563, +445T) từ baseline xấu

**Nguyên nhân:** Bootstrap quá ít data → `STDDEV()` trả NULL → fallback `AVG * 0.1` cho stddev rất nhỏ → division cho số gần 0.

**Fix đã apply:** `GREATEST(..., 1.0)` trong `baseline_learning.py` — floor stddev tối thiểu 1 phút.

### stddev_duration_minutes = 0 trong online features

**Nguyên nhân:** Welford cần ≥ 2 observations khác nhau. VietMap poll mỗi 5 phút → mỗi giờ có ít observations, đặc biệt giờ đầu.

**Không phải bug** — zscore vẫn tính được vì dùng `baseline_stddev`, không phải `window_stddev`.

### IForest = IFOREST-only khi zscore = NULL

**Nguyên nhân:** Baseline chưa có cho slot hiện tại (sau restart hoặc data thưa).

**Fix:** Chạy `make bootstrap` → `make train`. Baseline sẽ được compute cho tất cả slots có dữ liệu.

### zscore_threshold = 2.0 fallback khi chưa retrain

**Nguyên nhân:** `zscore_p99` column chỉ có sau lần `retrain` đầu tiên với code mới. Trước đó `row.get("zscore_p99")` trả `None` → fallback `2.0`.

**Action:** Sau khi deploy code mới, trigger `make train` (hoặc đợi Prefect `retrain` flow chạy tự động mỗi 6h). Sau đó restart online service để reload baseline với threshold mới.

### Window state mất khi restart online service

**Fix đã apply:** `_restore_windows()` — reconstruct `RouteWindow` từ Postgres khi startup. `M2 = stddev² × (count-1)`.

---

## Data Quality Checklist

Trước khi tin vào anomaly results:

- [ ] `gold.traffic_baseline` có rows cho tất cả 20 routes × 168 slots (7 ngày × 24 giờ)
- [ ] `gold.traffic_hourly` có ≥ 10 rows per route (IForest training minimum)
- [ ] Online service log: `baseline loaded — 20 entries` (không phải 0)
- [ ] Serving log: không có `no model for route` liên tục
- [ ] `duration_zscore` không phải NULL cho majority of routes
