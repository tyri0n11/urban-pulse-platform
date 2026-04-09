# Urban Pulse — Anomaly Detection Design

Hệ thống phát hiện bất thường dùng **dual-signal**: hai tầng độc lập với bài toán, dữ liệu, và tốc độ khác nhau. Kết quả chỉ đáng tin nhất khi cả hai đồng ý.

---

## Tổng quan: Online vs Offline

| Tiêu chí | Online (Z-Score) | Offline (IsolationForest) |
|----------|-----------------|--------------------------|
| **Câu hỏi** | "Duration route này ngay lúc này có bất thường không?" | "Tổ hợp features của route này có khác bình thường không?" |
| **Tính khi nào** | Mỗi Kafka message (~5 phút/lần) | On-demand khi serving query (SSE mỗi 15s) |
| **Nguồn dữ liệu** | Welford window hiện tại + gold baseline | 7 features (cyclical time + congestion) từ Postgres |
| **Chiều dữ liệu** | 1 chiều (duration) | 7 chiều đồng thời |
| **Điểm mạnh** | Real-time, interpretable | Bắt pattern phức tạp, context-aware theo giờ/ngày |
| **Điểm yếu** | Bỏ sót pattern phức tạp | Không có baseline rõ ràng |
| **Lưu ở đâu** | `online_route_features.is_anomaly` | `route_iforest_scores` (mỗi 15s qua SSE) |
| **Latency** | < 20s từ ingestion | ~100ms per route khi query |

---

## Signal 1: Z-Score (Speed Layer)

### Ý nghĩa

Z-score đo lường **số độ lệch chuẩn** so với lịch sử cùng thời điểm (giờ + ngày trong tuần):

```
zscore = (mean_duration_hiện_tại - baseline_mean) / baseline_stddev
```

Ví dụ: Thứ 2 lúc 15h, route A thường mất 68 phút (±10 phút). Hôm nay 15h route A mất 95 phút → zscore = (95 - 68) / 10 = **+2.7** — gần ngưỡng cảnh báo.

> **One-sided threshold**: Chỉ `zscore > 3.0` mới trigger anomaly — tắc nghẽn (duration cao hơn bình thường). Duration thấp hơn bình thường (zscore âm) không được coi là anomaly ở tầng này.

### Pipeline tính toán

```
Kafka message (duration_minutes)
  └─► RouteWindow.update()          # Welford: cập nhật mean, M2
        └─► zscore = (mean - baseline.mean) / baseline.stddev
              └─► is_anomaly = zscore > 3.0
                    └─► UPSERT → online_route_features
```

### Welford Online Algorithm

Tính mean và stddev mà không cần lưu raw observations (constant memory):

```python
delta  = x - mean
mean  += delta / count
delta2 = x - mean
M2    += delta * delta2
stddev = sqrt(M2 / (count - 1))   # Bessel's correction
```

- Reset khi qua giờ UTC mới (`window_start_ts` thay đổi)
- Cần ≥ 2 observations để có `stddev > 0`
- Restart-safe: `M2` reconstruct từ Postgres khi startup (`M2 = stddev² × (count-1)`)

### Baseline (gold.traffic_baseline)

Tính từ toàn bộ gold data, group by `(route_id, day_of_week, hour_of_day)`:

```sql
SELECT
    route_id,
    EXTRACT(DOW  FROM hour_utc) AS day_of_week,
    EXTRACT(HOUR FROM hour_utc) AS hour_of_day,
    AVG(avg_duration_minutes)   AS baseline_duration_mean,
    GREATEST(
        COALESCE(STDDEV(avg_duration_minutes), AVG(avg_duration_minutes) * 0.1),
        1.0
    )                           AS baseline_duration_stddev,
    AVG(avg_heavy_ratio)        AS baseline_heavy_ratio_mean
FROM gold.traffic_hourly
GROUP BY route_id, day_of_week, hour_of_day
```

**Load cycle:** Startup + tự refresh mỗi 6h.

---

## Signal 2: IsolationForest (Batch Layer)

### Ý nghĩa

IForest học **"vùng bình thường"** trong không gian 7 chiều từ lịch sử nhiều tuần. Điểm nằm ngoài vùng đó là anomaly — kể cả khi từng feature riêng lẻ trông ổn.

### Feature Vector — Cyclical Time Encoding

7 features cố định (thứ tự quan trọng, phải match `_FEATURE_ORDER` trong serving):

```python
FEATURE_COLUMNS = [
    "sin_hour",           # 1 — sin(2π × hour / 24)
    "cos_hour",           # 2 — cos(2π × hour / 24)
    "sin_dow",            # 3 — sin(2π × dow / 7)
    "cos_dow",            # 4 — cos(2π × dow / 7)
    "mean_heavy_ratio",   # 5 — tỷ lệ xe nặng
    "mean_moderate_ratio",# 6 — tỷ lệ xe tắc trung bình
    "max_severe_segments",# 7 — số đoạn nghiêm trọng nhất
]
```

**Tại sao cyclical encoding?**
Hour-of-day (0–23) và day-of-week (0–6) là circular. Encoding thẳng tạo khoảng cách giả tạo giữa `hour=23` và `hour=0` — thực ra 23h và 0h rất gần nhau. Sin/cos giữ tính liên tục:
```
sin_hour = sin(2π × hour / 24)
cos_hour = cos(2π × hour / 24)
```

**Không dùng `duration_zscore` và `baseline` nữa** — loại bỏ dependency vào `gold.traffic_baseline` khi training, giúp model học trực tiếp từ pattern thực tế theo giờ/ngày.

### Training Pipeline

```
gold.traffic_hourly ──► cyclical encoding ──► IsolationForest.fit()
                                                      ↓
                                         MLflow: traffic-anomaly-iforest@champion
```

- Train per-route, cần ≥ 10 samples
- `contamination=0.05` — model kỳ vọng 5% data là anomaly
- Trigger: Prefect `retrain` flow mỗi 6h → `POST ml-service:8000/train`

### Scoring (On-demand)

```python
now = datetime.utcnow()
X = np.array([[
    sin(2π × now.hour / 24),
    cos(2π × now.hour / 24),
    sin(2π × now.weekday() / 7),
    cos(2π × now.weekday() / 7),
    row["mean_heavy_ratio"],
    row["mean_moderate_ratio"],
    row["max_severe_segments"],
]])
iforest_anomaly = model.predict(X)[0] == -1      # -1 = anomaly
iforest_score   = model.decision_function(X)[0]  # âm hơn = bất thường hơn
```

> **Score interpretation**: `decision_function` trả về giá trị **âm cho anomaly**, **dương cho normal**. Ngưỡng là `0`. Không phải `> 0.5` hay `> threshold` — giá trị âm = bất thường.

Model load lazy per-route, TTL = 1h. Fallback về zscore-only nếu không có model.

### Persistence (route_iforest_scores)

IForest kết quả được lưu vào `route_iforest_scores` mỗi 15s qua SSE loop. Table dùng **majority vote** để giảm noise:

```sql
-- Aggregate: anomalous nếu >= 50% lần score trong window là anomaly
iforest_anomaly = anomaly_count::float / score_count >= 0.5
both_anomaly    = both_count::float    / score_count >= 0.5
```

Dùng cho: `/anomalies/history`, `/anomalies/summary`, `rag_indexer` (lấy anomaly events để index vào ChromaDB).

---

## Dual-Signal Merge

```python
zscore_anomaly  = row["is_anomaly"]          # từ online_route_features
iforest_anomaly = pred.iforest_anomaly       # from route_iforest_scores hoặc on-demand
both_anomaly    = zscore_anomaly and iforest_anomaly
```

### Ý nghĩa từng kết hợp

| zscore | iforest | Signal | Ý nghĩa | Hành động |
|--------|---------|--------|---------|-----------|
| ✅ | ✅ | `BOTH` | Cả hai đồng ý — cảnh báo tin cậy nhất | Telegram alert |
| ✅ | ❌ | `ZSCORE` | Duration tăng mạnh, pattern chưa lạ theo ML | Dashboard + log |
| ❌ | ✅ | `IFOREST` | Pattern lạ nhiều chiều, duration chưa vượt ngưỡng | Dashboard + log |
| ❌ | ❌ | — | Bình thường | — |

---

## Telegram Alerting

Prefect `alert` flow chạy **mỗi 5 phút**, chỉ alert khi có IForest-confirmed signal:

```python
# Chỉ alert IFOREST hoặc BOTH — zscore-only quá nhiều false positive
if row["iforest_anomaly"] or row["both_anomaly"]:
    if not recently_alerted(route_id, cooldown=30min):
        analysis = ollama_generate(route_context)   # 1 câu tiếng Việt
        send_telegram(alert_message)
        log_alert(route_id, signal, message)
```

Message format:
```
🚨 Urban Pulse Alert [17:30 UTC]
📍 Urban Core → Southern Port
Tín hiệu: BOTH
Heavy: 18.5% | Moderate: 32.1% | Severe segs: 3

💡 Cao điểm chiều thứ 2, lượng xe tải từ khu công nghiệp...
```

---

## RAG-Enhanced Explanation

Khi user nhấn "Explain" hoặc dùng `/rca`, hệ thống:

1. **Fetch live weather** từ Open-Meteo API (cache 15 phút) → inject trực tiếp vào prompt
2. **Embed câu hỏi** bằng `nomic-embed-text` (768-dim)
3. **Retrieve** từ ChromaDB:
   - `anomaly_events` — lịch sử bất thường 7 ngày, filter theo `route_id`
   - `traffic_patterns` — baseline (route × dow × hour) từ gold layer
   - `external_context` — thời tiết HCMC 7 ngày từ Open-Meteo, filter `hour_ts >= now-24h`
4. **Inject context** vào prompt Ollama theo thứ tự: live weather → RAG chunks
5. **Stream** response qua SSE

**Three-layer context:**

| Layer | Nguồn | Latency | Mục đích |
|-------|-------|---------|---------|
| Live weather | Open-Meteo API (direct) | ~200ms | Điều kiện thời tiết ngay lúc này |
| RAG weather | ChromaDB `external_context` | ~50ms | Pattern thời tiết 7 ngày gần nhất |
| RAG traffic | ChromaDB `anomaly_events` + `traffic_patterns` | ~50ms | Lịch sử bất thường + baseline |

LLM (qwen2.5:3b) được instruction rõ ràng: nếu đang mưa/giông → phân tích mối liên hệ với tắc nghẽn; nếu trời quang → loại trừ yếu tố thời tiết.

Kết quả tốt hơn plain LLM vì model có **evidence thực tế** (traffic history + weather) thay vì chỉ dựa vào parametric knowledge.

---

## Known Issues & Mitigations

### Extreme z-score từ baseline xấu

**Nguyên nhân:** Bootstrap quá ít data → `STDDEV()` trả NULL → fallback `AVG * 0.1` cho stddev rất nhỏ.

**Fix:** `GREATEST(..., 1.0)` trong `baseline_learning.py` — floor stddev tối thiểu 1 phút.

### 40% IForest anomaly rate sau khi deploy cyclical encoding

**Nguyên nhân:** Serving load model cũ (5 features), nhưng code mới build vector 7 features → dimension mismatch → mọi route bị flag.

**Fix:** Restart serving container sau khi retrain xong với model mới.

### IForest score hiển thị sai threshold trên UI

**Fix đã apply:** Label trên routes/[id] page: `anomaly if < 0` (thay vì `anomaly > 0.5`). Z-score threshold: `anomaly if > 3.0` (thay vì `±3.0`).

### Window state mất khi restart online service

**Fix đã apply:** `_restore_windows()` — reconstruct `RouteWindow` từ Postgres khi startup.

---

## Data Quality Checklist

Trước khi tin vào anomaly results:

- [ ] `gold.traffic_baseline` có rows cho tất cả 20 routes × 168 slots
- [ ] `gold.traffic_hourly` có ≥ 10 rows per route (IForest training minimum)
- [ ] Online service log: `baseline loaded — 20 entries`
- [ ] Serving log: không có `no model for route` liên tục
- [ ] `iforest_score` từ `/predict/anomalies/{route_id}` trả về giá trị trong range `[-0.5, 0.3]`
