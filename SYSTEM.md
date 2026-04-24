# Urban Pulse — System Overview

## What it is

Real-time traffic anomaly detection platform for Ho Chi Minh City. Polls VietMap API every 5 min, processes data through a medallion lakehouse, detects anomalies via dual-signal (z-score + IsolationForest), explains anomalies via RAG-enhanced LLM, and streams results to a Next.js dashboard via SSE.

## Hardware

- **Homelab**: 1 machine, Intel i5-8th gen, 24 GB RAM
- **Domain**: `tyr1on.io.vn` — Cloudflare DNS (A records `@` and `*` → 171.248.102.63, DNS-only)
- **Router**: Viettel ZTE F670Y — port forwarding 80 + 443 → 192.168.1.9
- **OS path**: `/opt/urban-pulse/` for all persistent volumes

---

## Repos

| Repo | Location | Purpose |
|------|----------|---------|
| `urban-pulse-platform` | `~/urban-pulse-platform` | Backend monorepo (all services) |
| `v0-urban-pulse-dashboard` | inside platform repo | Active UI (cloned as `v0-urban-pulse-dashboard/`) |

> The dashboard repo must live inside `urban-pulse-platform/` so docker-compose context path `../../v0-urban-pulse-dashboard` resolves correctly.

---

## Services

| Service | Image | Exposed (prod) | Purpose |
|---------|-------|---------------|---------|
| **traefik** | traefik:v2.11 | :80, :443 | Reverse proxy + Let's Encrypt TLS |
| **redpanda** | redpanda:v25.3.10 | internal | Kafka-compatible broker |
| **redpanda-console** | redpanda/console:v3.5.3 | `redpanda.tyr1on.io.vn` | Broker UI (basicauth) |
| **minio** | minio:latest | `minio.tyr1on.io.vn` | S3-compatible object store |
| **nessie** | projectnessie/nessie:0.99.0 | internal | Iceberg catalog |
| **dremio** | dremio/dremio-oss:latest | `dremio.tyr1on.io.vn` | SQL query engine |
| **postgres** | postgres:16-alpine | internal | Online feature store |
| **mlflow** | ghcr.io/mlflow/mlflow:v2.21.3 | `mlflow.tyr1on.io.vn` | Experiment tracking + model registry (basicauth) |
| **prefect-server** | prefecthq/prefect:3-latest | `prefect.tyr1on.io.vn` | Workflow orchestration UI (basicauth) |
| **chromadb** | chromadb/chroma:latest | internal | Vector DB for RAG pipeline |
| **ollama** | ollama/ollama:latest | internal | LLM + embedding inference |
| **traffic-ingestion** | custom | internal | Polls VietMap API every 5 min → Kafka (sequential, 10s inter-request delay) |
| **weather-ingestion** | custom | internal | Polls Open-Meteo API hourly → triggers batch weather pipeline |
| **streaming** | custom | internal | Kafka → MinIO bronze Parquet |
| **online** | custom | internal | Kafka → Postgres real-time features |
| **batch** | custom | internal | Prefect: medallion + retrain + RAG indexer + alerter |
| **ml** | custom | internal | FastAPI: IsolationForest train endpoint |
| **serving** | custom | `api.tyr1on.io.vn` | FastAPI REST + SSE + LLM + Telegram webhook |
| **ui** | custom (Next.js) | `tyr1on.io.vn` | Dashboard |
| **grafana** | grafana:11.6.0 | `grafana.tyr1on.io.vn` | Metrics + logs dashboards |
| **loki** | grafana/loki:3.4.2 | internal | Log aggregation |
| **promtail** | grafana/promtail:3.4.2 | internal | Log shipper |

> **Dev only**: Ollama runs natively on macOS (Metal GPU). Services point to `host.docker.internal:11434`. On prod, Ollama runs in Docker with NVIDIA GPU reservation.

---

## Data Flow

```
VietMap API (every 5 min)
  └─► ingestion ──► Kafka: traffic-route-bronze
                      ├─► streaming ──► MinIO bronze/year=Y/month=M/day=D/hour=H/*.parquet
                      └─► online    ──► Postgres: online_route_features (Welford + z-score)

Open-Meteo API (free, no key — HCMC lat/lon)
  └─► batch (1 hr)   ──► ChromaDB: external_context (7-day rolling hourly weather)
  └─► serving /chat  ──► live fetch (cache 15 min) ──► inject into LLM prompt

MinIO bronze
  └─► batch (5 min)  ──► Iceberg silver.traffic_route
  └─► batch (1 hr)   ──► Iceberg gold.traffic_hourly
                           └─► RAG index: anomaly_events + external_context/weather (ChromaDB)
  └─► batch (6 hrs)  ──► POST ml-service:8000/train
                           └─► MLflow: traffic-anomaly-iforest@champion
                                 └─► serving loads on /predict (TTL 1h)
                           └─► RAG index: traffic_patterns (ChromaDB, full gold scan)
  └─► batch (5 min)  ──► alert flow: check anomalies → Telegram (30-min cooldown)

Postgres + ChromaDB
  └─► serving /explain & /rca ──► RAG retrieval (anomaly + pattern + weather) + Ollama ──► SSE
  └─► serving /chat ──► live snapshot (traffic + weather) + Ollama ──► Stream SSE
  └─► serving /telegram/webhook ──► @tyr1on_system_alert_bot ──► Stream SSE

Postgres online_route_features
  └─► serving /events/traffic ──► SSE ──► UI dashboard (every 15s)
                                  └─► IForest scoring ──► route_iforest_scores
```

### Route IDs

Format: `zone{N}_{zone_name}_to_zone{M}_{zone_name}`
Example: `zone1_urban_core_to_zone4_southern_port`

6 zones: `urban_core`, `eastern_innovation`, `northern_industrial`, `southern_port`, `western_periurban`, `southern_coastal`. 20 routes total (see `routes.json`).

---

## Key Ports (dev)

| Service | Port |
|---------|------|
| Redpanda | 19092 |
| Redpanda Console | 8080 |
| MinIO API | 9000, Console 9001 |
| MLflow | 5000 |
| Prefect | 4200 |
| Postgres | 5432 |
| ML service | 8000 |
| Serving API | 8001 |
| ChromaDB | 8010 |
| Ollama | 11434 |
| Grafana | 3001 |
| Loki | 3100 |
| UI | 3000 |

---

## CORS

Serving allows: `localhost:3000`, `127.0.0.1:3000`, `https://tyr1on.io.vn`, `https://www.tyr1on.io.vn`

---

## Ollama Models

| Model | Purpose | Size |
|-------|---------|------|
| `qwen2.5:3b` | LLM inference (chat, explain, rca, alert) | 1.9 GB |
| `nomic-embed-text` | Embedding for RAG (768-dim) | 274 MB |

---

## MLflow constraint

`mlflow < 2.22` everywhere — server runs v2.21.3, newer clients hit 404s on changed endpoints.

---

## Postgres Schema

### online_route_features

Real-time Welford window + z-score, written by `online` service:

| Column | Type | Description |
|--------|------|-------------|
| `route_id` | TEXT | PK |
| `window_start` | TIMESTAMPTZ | PK — current UTC hour boundary |
| `observation_count` | INTEGER | Messages received in window |
| `mean_duration_minutes` | DOUBLE | Welford running mean |
| `stddev_duration_minutes` | DOUBLE | Welford running stddev |
| `mean_heavy_ratio` | DOUBLE | Running mean heavy vehicle ratio |
| `mean_moderate_ratio` | DOUBLE | Running mean moderate ratio |
| `max_severe_segments` | DOUBLE | Max severe segments in window |
| `duration_zscore` | DOUBLE | `(mean - baseline.mean) / baseline.stddev` |
| `is_anomaly` | BOOLEAN | `zscore > 3.0` |
| `last_ingest_lag_ms` | BIGINT | E2E Kafka→Postgres latency |

### route_iforest_scores

IForest scoring results (written by SSE loop every 15s, majority vote aggregation):

| Column | Type | Description |
|--------|------|-------------|
| `route_id` | TEXT | PK |
| `window_start` | TIMESTAMPTZ | PK |
| `iforest_score` | DOUBLE | `model.decision_function()` — negative = anomaly |
| `iforest_anomaly` | BOOLEAN | Latest single-shot result |
| `both_anomaly` | BOOLEAN | Latest zscore AND iforest |
| `score_count` | INTEGER | Times scored in this window |
| `anomaly_count` | INTEGER | Times scored as anomaly |
| `both_count` | INTEGER | Times scored as both |

### anomaly_alert_log

Telegram alert history for deduplication:

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL | PK |
| `alerted_at` | TIMESTAMPTZ | When alert was sent |
| `route_id` | TEXT | Route that triggered alert |
| `signal` | TEXT | `BOTH` or `IFOREST` |
| `message` | TEXT | Full Telegram message sent |

### prediction_history

Per-tick IForest scoring results + full E2E latency breakdown (one row per 15s scoring cycle):

| Column | Type | Description |
|--------|------|-------------|
| `id` | BIGSERIAL | PK |
| `scored_at` | TIMESTAMPTZ | When IForest predict() ran |
| `route_id` | TEXT | Route scored |
| `window_start` | TIMESTAMPTZ | UTC hour bucket from `online_route_features` |
| `iforest_score` | DOUBLE | `decision_function()` — negative = anomaly |
| `iforest_anomaly` | BOOLEAN | `predict() == -1` |
| `zscore_anomaly` | BOOLEAN | `is_anomaly` from `online_route_features` |
| `both_anomaly` | BOOLEAN | Both signals agree |
| `duration_zscore` | DOUBLE | Z-score value at time of scoring |
| `mean_duration_minutes` | DOUBLE | |
| `mean_heavy_ratio` | DOUBLE | |
| `ingest_lag_ms` | BIGINT | VietMap poll → Postgres write (pipeline input latency) |
| `staleness_ms` | BIGINT | Postgres write → IForest score (data age at scoring) |
| `scoring_ms` | BIGINT | IForest predict() wall clock |
| `full_e2e_ms` | BIGINT | ingest_lag_ms + staleness_ms + scoring_ms |

**Two SLOs tracked here:**
- **Pipeline processing latency** = `ingest_lag_ms + scoring_ms` — system overhead only, target p95 < 60 s
- **Data freshness** = `staleness_ms` — bounded by VietMap 5-min poll interval, target p95 < 310 s

### rag_interaction_log

RCA query log for fine-tuning data collection:

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL | PK |
| `ts` | TIMESTAMPTZ | Query timestamp |
| `query_type` | TEXT | `route_rca` or `free_text` |
| `route_id` | TEXT | Route queried (nullable) |
| `query` | TEXT | User question |
| `retrieved_chunks` | JSONB | RAG chunks injected into prompt |
| `response` | TEXT | LLM response |
| `feedback` | SMALLINT | `1` (good) or `-1` (bad), nullable |
| `lang` | TEXT | `vi` or `en` |

---

## IsolationForest Feature Vector

7 features (cyclical time encoding), fixed order:

```python
["sin_hour", "cos_hour", "sin_dow", "cos_dow",
 "mean_heavy_ratio", "mean_moderate_ratio", "max_severe_segments"]
```

Training source: `gold.traffic_hourly` with cyclical transformations.
Serving source: current `online_route_features` + `datetime.utcnow()` for time encoding.

---

## ChromaDB Collections (RAG)

| Collection | Docs | Metadata filters | Content | Refresh |
|-----------|------|-----------------|---------|---------|
| `anomaly_events` | 500+ | `route_id`, `hour`, `dow` | Lịch sử bất thường 7 ngày từ Postgres | Mỗi 1h |
| `traffic_patterns` | 3360 | `route_id`, `hour`, `dow` | Baseline (route × dow × hour) từ gold | Mỗi 6h |
| `external_context` | 192 | `hour`, `hour_ts`, `is_rain`, `is_storm`, `context_type` | Thời tiết HCMC 7 ngày + 1 ngày tương lai từ Open-Meteo | Mỗi 1h |

Embedding model: `nomic-embed-text` via Ollama `/api/embed`. Space: cosine.

**Retrieval strategy:**
- `anomaly_events` + `traffic_patterns`: filter by `route_id`, semantic query
- `external_context`: filter by `hour_ts >= now-24h` (recent weather ưu tiên); fallback về `{"hour": hour}` nếu collection rỗng

---

## Prefect Flows

| Flow | Schedule | What it does |
|------|----------|-------------|
| `microbatch` | Every 5 min | Bronze → Silver (incremental) |
| `hourly-gold` | Every 1 hr | Silver → Gold + RAG index (anomaly events + weather from Open-Meteo) |
| `retrain` | Every 6 hrs | Gold → baseline + POST /train + RAG index (traffic patterns full scan) |
| `alert` | Every 5 min | Check active anomalies → Telegram (IForest-confirmed only) |
| `rag-index` | Manual | Full re-index; `--param index_patterns=false` = weather + anomalies only (~60s) |
| `bootstrap` | Manual | Full medallion rebuild from scratch |
| `backfill` | Manual | Time-range backfill for silver |

---

## Kafka Consumer Config

| Service | `auto.offset.reset` | `enable.auto.commit` |
|---------|--------------------|--------------------|
| `streaming` | `earliest` | `False` (manual, after MinIO flush) |
| `online` | `earliest` | `False` (manual, after Postgres upsert) |

---

## Failure Handling

| Layer | Behavior |
|-------|----------|
| streaming → MinIO | Buffer cleared only after successful upload; offset committed after flush |
| online → Postgres | Offset committed after upsert; `OperationalError` triggers reconnect + retry |
| batch tasks | Prefect `retries=2, retry_delay_seconds=30` |
| RAG indexer | Upsert-based — safe to re-run; timeout protected per batch of 20 docs |
| Telegram alert | Best-effort; failure logged but doesn't break flow |
| serving model load | Falls back to zscore-only if no MLflow model for route |
| RAG retrieval in /explain | Best-effort; proceeds without context if ChromaDB unavailable |
| traffic-ingestion → VietMap | Per-route exception caught; cycle continues with remaining routes. Failed routes logged as ERROR and counted in Grafana. |

---

## Observability

### Grafana Dashboards

| Dashboard | Key metrics |
|-----------|-------------|
| `ingestion-service` | Cycle duration, routes fetched/cycle, VietMap API latency p50/p90/p95, 429 rate limit hits |
| `online-features` | IForest SLO (pipeline latency p95 < 60s, data freshness p95 < 310s), scoring breakdown |
| `streaming-service` | Kafka consumer lag, MinIO write rate |
| `batch-service` | Prefect flow run status, medallion pipeline duration |
| `serving-api` | Request rate, LLM latency, SSE connections |
| `anomaly-detection` | Anomaly rate per route, dual-signal breakdown |
| `system-health` | Container health, memory/CPU |

### Promtail Log Collection

Promtail scrapes stdout from these containers (via Docker socket):

```
traffic-ingestion-service, weather-ingestion-service,
streaming-service, batch-service, ml-service, online-service, serving-service
```

Labels extracted: `service` (container name), `stream` (stdout/stderr), `level` (INFO/ERROR/WARNING from log format).

---

## Bootstrap / Operations Order

Fresh environment:
```bash
make dev
make bootstrap    # bronze→silver→gold→baseline
make train        # POST /train → MLflow
# After Ollama models pulled — seed RAG index (weather + anomalies only, fast):
docker exec batch-service .venv/bin/prefect deployment run rag-index/rag-index-deployment \
  --param index_patterns=false
# Traffic patterns index happens automatically on next retrain (every 6h)
```

### Why routes_skipped=20 in MLflow

`build_features()` LEFT JOIN gold × baseline → if `gold.traffic_baseline` is empty, all rows filter out → 0 features → every route skips. Fix: run `make bootstrap` then `make train`.
