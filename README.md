# Urban Pulse Platform

![OVERVIEW](assets/urbanpulse.png)


Real-time traffic anomaly detection platform cho Thành phố Hồ Chí Minh. Polls VietMap API mỗi 5 phút, xử lý qua medallion lakehouse, phát hiện bất thường bằng dual-signal (Z-Score + IsolationForest), stream kết quả lên dashboard Next.js qua SSE, và giải thích bằng LLM qua RAG pipeline.

**Homelab deployment:** Intel i5-8th gen, 24 GB RAM · `tyr1on.io.vn`

---

## Architecture

```
VietMap API (mỗi 5 phút)
  └─► ingestion ──► Kafka: traffic-route-bronze
                      ├─► streaming ──► MinIO: bronze/year=Y/month=M/day=D/hour=H/*.parquet
                      └─► online    ──► Postgres: online_route_features (z-score, Welford)

MinIO bronze
  └─► batch (mỗi 5 phút) ──► Iceberg: silver.traffic_route
  └─► batch (mỗi 1 giờ)  ──► Iceberg: gold.traffic_hourly + RAG index (anomaly events)
  └─► batch (mỗi 6 giờ)  ──► POST /train → MLflow: iforest@champion + RAG index (traffic patterns)
  └─► batch (mỗi 5 phút) ──► Anomaly alerter → Telegram (IForest-confirmed anomalies)

Postgres + ChromaDB (RAG)
  └─► serving /explain ──► RAG retrieval + Ollama → LLM explanation (SSE)
  └─► serving /rca     ──► RAG retrieval + Ollama → Root Cause Analysis (SSE)
  └─► serving /chat    ──► Live snapshot + Ollama → Conversational AI (SSE)
  └─► Telegram bot     ──► @tyr1on_system_alert_bot → chat with live data
```

### Dual-signal anomaly detection

| Signal | Tầng | Threshold | Latency |
|--------|------|-----------|---------|
| **Z-Score** | Online (Kafka consumer) | `z > 3.0` | < 20s |
| **IsolationForest** | Serving (on-demand) | `decision_function < 0` (7 cyclical features) | ~100ms |

`both_anomaly = true` khi cả hai đồng ý → cảnh báo tin cậy nhất. Chi tiết: [ANOMALY.md](ANOMALY.md)

### RAG Pipeline (LLM Explain)

```
Batch (hourly) ──► ChromaDB collections:
  ├─ anomaly_events    (441+ docs) — lịch sử bất thường 7 ngày gần nhất
  ├─ traffic_patterns  (3360+ docs) — baseline theo (route, dow, hour)
  └─ external_context  — thời tiết / sự kiện (reserved)

serving /explain & /rca:
  Query → OllamaEmbedder (nomic-embed-text, 768-dim)
         → ChromaDB cosine retrieval
         → Inject context → Ollama (qwen2.5:3b) → Stream SSE
```

---

## Monorepo Layout

```
apps/
  ingestion/   # Poll VietMap API → Kafka
  streaming/   # Kafka → MinIO bronze Parquet
  batch/       # Prefect: medallion pipeline + retrain + RAG indexer + alerter
  ml/          # FastAPI: train IsolationForest, log MLflow
  online/      # Kafka → Postgres real-time features (Welford + z-score)
  serving/     # FastAPI REST + SSE: /online, /anomalies, /metrics, /predict, /rca, /chat

packages/
  core/            # Pydantic models + Settings
  infra-clients/   # Kafka, MinIO, DuckDB, PyIceberg, MLflow client factories
  observability/   # Logging utilities
  rag/             # ChromaDB client, embedder, collections, indexer, retriever

infra/
  docker/          # docker-compose.dev.yaml + docker-compose.prod.yaml
  monitoring/      # Prometheus, Grafana dashboards, Loki, Promtail

platform/schemas/  # JSON Schema cho Kafka message formats
```

---

## Services & Ports (Dev)

| Service | Port | Mô tả |
|---------|------|-------|
| Redpanda (Kafka) | 19092 | External; internal: `redpanda:9092` |
| Redpanda Console | 8080 | Kafka UI |
| MinIO API | 9000 | Object storage |
| MinIO Console | 9001 | MinIO UI |
| Nessie | 19120 | Iceberg catalog |
| Dremio | 9047 | SQL query engine |
| PostgreSQL | 5432 | Online feature store |
| MLflow | 5000 | Model registry |
| Prefect Server | 4200 | Workflow orchestration |
| ML service | 8000 | FastAPI: `/train`, `/health` |
| Serving API | 8001 | FastAPI: REST + SSE + LLM |
| ChromaDB | 8010 | Vector DB (RAG) |
| Ollama | 11434 | LLM inference (`qwen2.5:3b`, `nomic-embed-text`) |
| Grafana | 3001 | Metrics + logs |
| Loki | 3100 | Log aggregation |
| Next.js UI | 3000 | Dashboard (sibling repo) |

> **Dev**: Ollama chạy natively trên macOS (Metal GPU), không phải Docker container. Các services trỏ đến `host.docker.internal:11434`.

---

## Quick Start (Dev)

### Prerequisites

- Docker Desktop (Mac) hoặc Docker Engine + Compose (Linux)
- `uv` — Python package manager
- Ollama (native, macOS): `brew install ollama`

### 1. Clone

```bash
git clone git@github.com:tyri0n11/urban-pulse-platform.git
cd urban-pulse-platform
git clone git@github.com:tyri0n11/v0-urban-pulse-dashboard.git
```

> UI repo phải nằm trong `urban-pulse-platform/` — docker-compose context path `../../v0-urban-pulse-dashboard`.

### 2. Tạo `.env`

```bash
cp .env.example .env
```

Điền vào:

```env
VIETMAP_API_KEY=<your key>
MINIO_ROOT_USER=<username>
MINIO_ROOT_PASSWORD=<password, không dùng ký tự @>
NESSIE_S3_ACCESS_KEY=<same as MINIO_ROOT_USER>
NESSIE_S3_SECRET_KEY=<same as MINIO_ROOT_PASSWORD>
POSTGRES_USER=urbanpulse
POSTGRES_PASSWORD=<password, không dùng ký tự @>
```

> **Không dùng `@` trong password** — asyncpg parse DSN và `@` phá vỡ host separator.

### 3. Pull Ollama models

```bash
ollama pull qwen2.5:3b
ollama pull nomic-embed-text
```

### 4. Start stack

```bash
make dev
```

Đợi ~2 phút để các services healthy. Kiểm tra:

```bash
make status
make logs
```

### 5. Bootstrap (lần đầu)

```bash
make bootstrap   # Tạo Iceberg tables, chạy medallion pipeline từ đầu
make train       # Train IsolationForest, push lên MLflow
```

### 6. Khởi tạo RAG index (lần đầu)

```bash
docker exec batch-service .venv/bin/prefect deployment run rag-index/rag-index-deployment
```

### 7. Mở UI

```
http://localhost:3000        — Dashboard
http://localhost:8001/docs   — Serving API docs
http://localhost:5000        — MLflow
http://localhost:4200        — Prefect
http://localhost:9001        — MinIO Console
http://localhost:3001        — Grafana
http://localhost:8010        — ChromaDB (RAG vector store)
```

---

## Development Commands

```bash
# Stack
make dev            # Start toàn bộ stack
make down           # Stop tất cả containers
make status         # docker ps
make logs           # Tail logs

# Build
make build          # Rebuild tất cả images
make build-serving  # Rebuild serving only
make build-ingestion

# ML
make train          # Trigger retrain thủ công
make bootstrap      # Full medallion rebuild từ đầu

# Code quality
make lint           # ruff check
make typecheck      # mypy strict
make test           # pytest (unit + integration)
make test-unit
make test-integration

# Routes
python routes_generator.py  # Tái tạo routes.json từ zones.json
```

### Rebuild và restart 1 service

```bash
docker compose --env-file .env -f infra/docker/docker-compose.dev.yaml build --no-cache serving
docker compose --env-file .env -f infra/docker/docker-compose.dev.yaml up -d serving
```

### Trigger Prefect flow thủ công

```bash
docker exec batch-service .venv/bin/prefect deployment run microbatch/microbatch-deployment
docker exec batch-service .venv/bin/prefect deployment run retrain/retrain-deployment
docker exec batch-service .venv/bin/prefect deployment run rag-index/rag-index-deployment
```

---

## Serving API Endpoints

Base URL (dev): `http://localhost:8001`

```
GET  /online/features                    — Latest snapshot per route
GET  /online/features/{route_id}         — Single route latest
GET  /online/features/{route_id}/history — Per-hour windows, ?hours=24
GET  /online/routes                      — Latest + static coords từ routes.json
GET  /online/lag                         — p50/p95/max ingest lag stats

GET  /anomalies/current                  — Anomalous routes ngay lúc này
GET  /anomalies/history                  — Anomaly events, ?hours=24
GET  /anomalies/summary                  — Count per hour
GET  /anomalies/{route_id}               — Full timeline 1 route
GET  /anomalies/{route_id}/explain       — SSE: LLM explanation (RAG-enhanced)

GET  /metrics/routes                     — Lightweight snapshot (all routes)
GET  /metrics/routes/{route_id}          — Per-hour trend
GET  /metrics/zones                      — Aggregate by zone
GET  /metrics/leaderboard                — Top N by |zscore|
GET  /metrics/heatmap                    — Route × hour zscore matrix

GET  /predict/anomalies                  — IForest score all routes
GET  /predict/anomalies/{route_id}       — IForest score 1 route
GET  /predict/model/info                 — Cache age, loaded URI, next refresh

POST /rca                                — RAG-powered Root Cause Analysis (SSE)
POST /rca/feedback                       — Thumbs up/down cho fine-tuning
GET  /rca/logs                           — Recent interaction logs

POST /chat                               — Conversational AI với live snapshot (SSE)
GET  /chat/context                       — Raw snapshot (debug)

POST /telegram/webhook                   — Telegram bot webhook
GET  /telegram/info                      — Bot info (verify token)
GET  /telegram/set-webhook?url=...       — Register webhook URL

GET  /events/traffic                     — SSE: traffic snapshot mỗi 15s

GET  /health/live
GET  /health/ready
```

---

## Data Layer: Medallion Architecture

```
bronze/   MinIO Parquet        — Raw VietMap observations, partitioned year/month/day/hour
silver/   Iceberg (Nessie)     — Cleaned, deduplicated per (route_id, timestamp_utc)
gold/     Iceberg (Nessie)     — Hourly aggregates + baseline stats
```

### Key tables

| Table | Layer | Mô tả |
|-------|-------|-------|
| `silver.traffic_route` | Silver | Cleaned observations từ bronze |
| `gold.traffic_hourly` | Gold | Aggregates per (route_id, hour_utc) |
| `gold.traffic_baseline` | Gold | Stats per (route_id, dow, hour): mean, stddev cho z-score |
| `online_route_features` | Postgres | Real-time Welford window + z-score |
| `route_iforest_scores` | Postgres | IForest scores mỗi 15s (persistence, anomaly history) |
| `anomaly_alert_log` | Postgres | Lịch sử Telegram alerts (dedup 30 phút) |
| `rag_interaction_log` | Postgres | Lịch sử RCA queries cho fine-tuning |

### Batch flows (Prefect)

| Flow | Schedule | Mô tả |
|------|----------|-------|
| `microbatch` | Mỗi 5 phút | Bronze → Silver (incremental) |
| `hourly-gold` | Mỗi 1 giờ | Silver → Gold + RAG index (anomaly events) |
| `retrain` | Mỗi 6 giờ | Gold → baseline + POST `/train` + RAG index (traffic patterns) |
| `alert` | Mỗi 5 phút | Check anomalies → Telegram alert (30-min cooldown per route) |
| `rag-index` | Manual | Full RAG re-index (anomalies + patterns) |
| `bootstrap` | Manual | Full rebuild từ đầu |
| `backfill` | Manual | Time-range backfill cho silver |

---

## Route Format

```
zone{N}_{zone_name}_to_zone{M}_{zone_name}
Ví dụ: zone1_urban_core_to_zone4_southern_port
```

6 zones: `urban_core`, `eastern_innovation`, `northern_industrial`, `southern_port`, `western_periurban`, `southern_coastal`

20 routes (không phải tất cả 30 cặp). `routes.json` là source of truth — tạo lại bằng `python routes_generator.py`.

---

## Toolchain

| Tool | Mục đích |
|------|---------|
| `uv` | Python package manager, workspace |
| `hatchling` | Build backend |
| `ruff` | Linter (line-length 100, target py312) |
| `mypy --strict` | Type checker |
| `pytest` | Tests với `unit` và `integration` markers |
| `confluent_kafka` | Kafka producer/consumer |
| `pyiceberg` | Iceberg table read/write |
| `duckdb` | In-process analytics (feature engineering) |
| `mlflow` | Experiment tracking + model registry (`< 2.22`) |
| `chromadb` | Vector database (RAG) |
| `httpx` | Async HTTP (Ollama, Telegram API) |

---

## Key Design Decisions

**Tại sao Welford thay vì buffer?**
O(1) memory bất kể throughput. Restart-safe vì reconstruct từ `mean + stddev + count` đã lưu trong Postgres.

**Tại sao IForest per-route thay vì global?**
Mỗi route có đặc trưng khác nhau (duration 15–90 phút). Global model sẽ bias về routes phổ biến.

**Tại sao cyclical time encoding (sin/cos)?**
Day-of-week và hour-of-day là circular variables. Encoding thẳng (0–23, 0–6) tạo khoảng cách giả tạo giữa cuối và đầu chu kỳ. Sin/cos giữ tính liên tục: `hour=23` gần `hour=0`.

**Tại sao RAG thay vì fine-tuning?**
Fine-tuning cần nhiều labeled data và compute. RAG inject lịch sử thực tế vào context ngay lập tức — không cần retrain, luôn up-to-date với dữ liệu mới nhất.

**Tại sao dual-signal thay vì chỉ 1?**
Z-score nhanh và interpretable nhưng chỉ nhìn 1 chiều (duration). IForest bắt được pattern phức tạp đa chiều. `both_anomaly` giảm false positive.

**Tại sao `mlflow < 2.22`?**
Server chạy v2.21.3. Client mới hơn hit 404 trên changed endpoints. Pin version toàn workspace.

---

## Docs

| File | Nội dung |
|------|---------|
| [ANOMALY.md](ANOMALY.md) | Dual-signal design, z-score, IForest cyclical features |
| [ML.md](ML.md) | Feature engineering chi tiết, Welford, RAG pipeline |
| [SYSTEM.md](SYSTEM.md) | Infrastructure, ports, service URLs, schema tables |
| [DEPLOY.md](DEPLOY.md) | Production deployment (Traefik + TLS + RAG setup) |
| [CLAUDE.md](CLAUDE.md) | Hướng dẫn cho AI assistant |

---

## Production

Deploy lên homelab qua Traefik + Let's Encrypt. Chi tiết: [DEPLOY.md](DEPLOY.md)

| URL | Service |
|-----|---------|
| `https://tyr1on.io.vn` | Dashboard |
| `https://api.tyr1on.io.vn` | Serving API + Telegram webhook |
| `https://grafana.tyr1on.io.vn` | Grafana |
| `https://mlflow.tyr1on.io.vn` | MLflow |
| `https://prefect.tyr1on.io.vn` | Prefect |
| `https://minio.tyr1on.io.vn` | MinIO |
| `https://redpanda.tyr1on.io.vn` | Redpanda Console |
| `https://dremio.tyr1on.io.vn` | Dremio |
