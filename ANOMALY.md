# Urban Pulse — Anomaly Detection Design

The system detects anomalies using a **dual-signal** approach: two independent layers with different algorithms, data sources, and response speeds. Results are most reliable when both signals agree.

---

## Overview: Online vs Offline

| Criterion | Online (Z-Score) | Offline (IsolationForest) |
|-----------|-----------------|--------------------------|
| **Question** | "Is this route's duration abnormal right now?" | "Is this route's feature combination unusual?" |
| **Computed** | Every Kafka message (~5 min interval) | On-demand when serving queries (SSE every 15s) |
| **Data source** | Current Welford window + gold baseline | 7 features (cyclical time + congestion) from Postgres |
| **Dimensionality** | 1D (duration) | 7D simultaneously |
| **Strengths** | Real-time, interpretable | Catches complex patterns, context-aware by hour/day |
| **Weaknesses** | Misses complex multi-dimensional patterns | No explicit baseline |
| **Stored in** | `online_route_features.is_anomaly` | `route_iforest_scores` (every 15s via SSE) |
| **Latency** | < 20s from ingestion | ~100ms per route at query time |

---

## Signal 1: Z-Score (Speed Layer)

### Meaning

Z-score measures **number of standard deviations** from the historical baseline for the same time slot (hour + day of week):

```
zscore = (current_mean - baseline_mean) / baseline_stddev
```

Example: On Monday at 3 PM, route A normally takes 68 minutes (±10 min). Today at 3 PM route A takes 95 minutes → zscore = (95 - 68) / 10 = **+2.7** — approaching the alert threshold.

> **One-sided threshold**: Only `zscore > 3.0` triggers an anomaly — congestion (duration higher than normal). Lower-than-normal duration (negative zscore) is not considered an anomaly at this layer.

### Computation Pipeline

```
Kafka message (duration_minutes)
  └─► RouteWindow.update()          # Welford: update mean, M2
        └─► zscore = (mean - baseline.mean) / baseline.stddev
              └─► is_anomaly = zscore > 3.0
                    └─► UPSERT → online_route_features
```

### Welford Online Algorithm

Computes mean and stddev per message without storing raw observations (constant memory):

```python
delta  = x - mean
mean  += delta / count
delta2 = x - mean
M2    += delta * delta2
stddev = sqrt(M2 / (count - 1))   # Bessel's correction
```

- Resets when a new UTC hour begins (`window_start_ts` changes)
- Requires ≥ 2 observations to produce `stddev > 0`
- Restart-safe: `M2` is reconstructed from Postgres on startup (`M2 = stddev² × (count-1)`)

### Baseline (gold.traffic_baseline)

Computed from all gold data, grouped by `(route_id, day_of_week, hour_of_day)`:

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

**Load cycle:** Startup + automatic refresh every 6h.

---

## Signal 2: IsolationForest (Batch Layer)

### Meaning

IForest learns the **"normal region"** in 7-dimensional space from weeks of history. Points outside this region are anomalies — even if each individual feature looks fine on its own.

### Feature Vector — Cyclical Time Encoding

7 fixed features (order matters, must match `_FEATURE_ORDER` in serving):

```python
FEATURE_COLUMNS = [
    "sin_hour",           # 1 — sin(2π × hour / 24)
    "cos_hour",           # 2 — cos(2π × hour / 24)
    "sin_dow",            # 3 — sin(2π × dow / 7)
    "cos_dow",            # 4 — cos(2π × dow / 7)
    "mean_heavy_ratio",   # 5 — heavy vehicle ratio
    "mean_moderate_ratio",# 6 — moderate congestion ratio
    "max_severe_segments",# 7 — most severely congested segments count
]
```

**Why cyclical encoding?**
Hour-of-day (0–23) and day-of-week (0–6) are circular variables. Integer encoding creates artificial distance between `hour=23` and `hour=0` — but 11 PM and midnight are actually very close. Sin/cos preserves continuity:
```
sin_hour = sin(2π × hour / 24)
cos_hour = cos(2π × hour / 24)
```

**`duration_zscore` and `baseline` are no longer used** — removing the dependency on `gold.traffic_baseline` at training time lets the model learn directly from real temporal patterns.

### Training Pipeline

```
gold.traffic_hourly ──► cyclical encoding ──► IsolationForest.fit()
                                                      ↓
                                         MLflow: traffic-anomaly-iforest@champion
```

- Trained per-route, requires ≥ 10 samples
- `contamination=0.05` — model expects 5% of training data to be anomalous
- Trigger: Prefect `retrain` flow every 6h → `POST ml-service:8000/train`

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
iforest_score   = model.decision_function(X)[0]  # more negative = more anomalous
```

> **Score interpretation**: `decision_function` returns **negative values for anomalies**, **positive for normal**. Threshold is `0`. Not `> 0.5` or `> threshold` — negative values mean anomalous.

Model is loaded lazily per-route with a 1-hour TTL. Falls back to zscore-only if no model is available.

### Persistence (route_iforest_scores)

IForest results are written to `route_iforest_scores` every 15s via the SSE loop. The table uses **majority vote** to reduce noise:

```sql
-- Aggregate: anomalous if >= 50% of scoring cycles in the window flagged it
iforest_anomaly = anomaly_count::float / score_count >= 0.5
both_anomaly    = both_count::float    / score_count >= 0.5
```

Used by: `/anomalies/history`, `/anomalies/summary`, `rag_indexer` (reads anomaly events for ChromaDB indexing).

---

## Dual-Signal Merge

```python
zscore_anomaly  = row["is_anomaly"]          # from online_route_features
iforest_anomaly = pred.iforest_anomaly       # from route_iforest_scores or on-demand
both_anomaly    = zscore_anomaly and iforest_anomaly
```

### What each combination means

| zscore | iforest | Signal | Meaning | Action |
|--------|---------|--------|---------|--------|
| ✅ | ✅ | `BOTH` | Both agree — most reliable alert | Telegram alert |
| ✅ | ❌ | `ZSCORE` | Duration spiked, pattern not unusual per ML | Dashboard + log |
| ❌ | ✅ | `IFOREST` | Multi-dimensional unusual pattern, duration not yet over threshold | Dashboard + log |
| ❌ | ❌ | — | Normal | — |

---

## Telegram Alerting

Prefect `alert` flow runs **every 5 minutes**, alerts only on IForest-confirmed signals:

```python
# Alert only on IFOREST or BOTH — zscore-only has too many false positives
if row["iforest_anomaly"] or row["both_anomaly"]:
    if not recently_alerted(route_id, cooldown=30min):
        analysis = ollama_generate(route_context)   # single-sentence summary
        send_telegram(alert_message)
        log_alert(route_id, signal, message)
```

Message format:
```
🚨 Urban Pulse Alert [17:30 UTC]
📍 Urban Core → Southern Port
Signal: BOTH
Heavy: 18.5% | Moderate: 32.1% | Severe segs: 3

💡 Monday afternoon peak, heavy vehicles from industrial zone...
```

---

## RAG-Enhanced Explanation

When a user clicks "Explain" or calls `/rca`, the system:

1. **Fetches live weather** from Open-Meteo API (15-min cache) → injects directly into prompt
2. **Embeds the query** using `nomic-embed-text` (768-dim)
3. **Retrieves** from ChromaDB:
   - `anomaly_events` — 7-day anomaly history, filtered by `route_id`
   - `traffic_patterns` — baseline (route × dow × hour) from gold layer
   - `external_context` — HCMC weather 7 days from Open-Meteo, filtered `hour_ts >= now-24h`
4. **Injects context** into Ollama prompt in order: live weather → RAG chunks
5. **Streams** response via SSE

**Three-layer context:**

| Layer | Source | Latency | Purpose |
|-------|--------|---------|---------|
| Live weather | Open-Meteo API (direct) | ~200ms | Current weather conditions |
| RAG weather | ChromaDB `external_context` | ~50ms | 7-day weather patterns |
| RAG traffic | ChromaDB `anomaly_events` + `traffic_patterns` | ~50ms | Anomaly history + baseline |

The LLM (qwen2.5:3b) is explicitly instructed: if it's raining/stormy → analyze the link to congestion; if clear → rule out weather as a factor.

Results are better than plain LLM because the model has **real evidence** (traffic history + weather) rather than relying solely on parametric knowledge.

---

## Conversational AI (/chat)

Beyond explain and rca (single-turn, route-specific), Urban Pulse provides `/chat` — a conversational AI assistant covering the entire system.

**Difference from /explain:**

| | `/explain` + `/rca` | `/chat` |
|-|--------------------|---------|
| Scope | One specific route | Entire system |
| Context | RAG retrieval (ChromaDB) | Live system snapshot (Postgres query) |
| Multi-turn | No | Yes — session history |
| Ollama API | `/api/generate` (stateless) | `/api/chat` (multi-turn messages array) |

**Session history:** Frontend sends conversation history (up to 10 most recent messages) in each request. Backend caps at 10 to avoid overflowing the context window of qwen2.5:3b (32k tokens). Session scope is widget lifetime — not persisted to DB.

**System snapshot:** Each `/chat` request pulls a live snapshot from Postgres:
- Top anomalous routes (is_anomaly = true)
- Aggregate metrics (heavy ratio p95, anomaly count)
- Live weather from Open-Meteo (cache 15 min)

The snapshot is injected into the system prompt before calling Ollama, ensuring responses are grounded in real data rather than parametric knowledge.

---

## Known Issues & Mitigations

### Extreme z-score from bad baseline

**Cause:** Bootstrap ran with too little data → `STDDEV()` returns NULL → fallback `AVG * 0.1` gives very small stddev.

**Fix:** `GREATEST(..., 1.0)` in `baseline_learning.py` — floor stddev at 1 minute minimum.

### 40% IForest anomaly rate after deploying cyclical encoding

**Cause:** Serving loaded the old model (5 features), but new code builds a 7-feature vector → dimension mismatch → every route flagged.

**Fix:** Restart serving container after retrain completes with the new model.

### IForest score displayed with wrong threshold in UI

**Fix applied:** Label on routes/[id] page: `anomaly if < 0` (not `anomaly > 0.5`). Z-score threshold: `anomaly if > 3.0` (not `±3.0`).

### Window state lost on online service restart

**Fix applied:** `_restore_windows()` — reconstructs `RouteWindow` from Postgres on startup.

---

## Data Quality Checklist

Before trusting anomaly results:

- [ ] `gold.traffic_baseline` has rows for all 20 routes × 168 time slots
- [ ] `gold.traffic_hourly` has ≥ 10 rows per route (IForest training minimum)
- [ ] Online service log: `baseline loaded — 20 entries`
- [ ] Serving log: no continuous `no model for route` messages
- [ ] `iforest_score` from `/predict/anomalies/{route_id}` returns values in range `[-0.5, 0.3]`
