# Urban Pulse Platform

![OVERVIEW](assets/urbanpulse.png)


Real-time traffic anomaly detection platform for Ho Chi Minh City. Polls VietMap API every 5 minutes, processes data through a medallion lakehouse, detects anomalies via dual-signal (Z-Score + IsolationForest), streams results to a Next.js dashboard via SSE, and explains anomalies via LLM with RAG pipeline backed by historical traffic and live weather data (Open-Meteo).

**Homelab deployment:** Intel i5-8th gen, 24 GB RAM · [tyr1on.io.vn](tyr1on.io.vn)

---

## Architecture

```
VietMap API (every 5 min)
  └─► ingestion ──► Kafka: traffic-route-bronze
                      ├─► streaming ──► MinIO: bronze/year=Y/month=M/day=D/hour=H/*.parquet
                      └─► online    ──► Postgres: online_route_features (z-score, Welford)

Open-Meteo API (free, no key)
  └─► batch (every 1 hr) ──► ChromaDB: external_context (7-day rolling weather)
  └─► serving /chat      ──► live fetch → inject directly into LLM prompt (cache 15 min)

MinIO bronze
  └─► batch (every 5 min) ──► Iceberg: silver.traffic_route
  └─► batch (every 1 hr)  ──► Iceberg: gold.traffic_hourly
                                └─► RAG index: anomaly_events + external_context (weather)
  └─► batch (every 6 hrs) ──► POST /train → MLflow: iforest@champion
                                └─► RAG index: traffic_patterns (full re-index)
  └─► batch (every 5 min) ──► Anomaly alerter → Telegram (IForest-confirmed anomalies)

Postgres + ChromaDB (RAG)
  └─► serving /explain ──► RAG retrieval (anomaly history + patterns + weather) + Ollama → SSE
  └─► serving /rca     ──► RAG retrieval + Ollama → Root Cause Analysis (SSE)
  └─► serving /chat    ──► Live snapshot (traffic + live weather) + Ollama → Conversational AI (SSE)
  └─► Telegram bot     ──► @tyr1on_system_alert_bot → chat with live data
```

### Dual-signal anomaly detection

| Signal | Layer | Threshold | Latency |
|--------|-------|-----------|---------|
| **Z-Score** | Online (Kafka consumer) | `z > 3.0` | < 20s |
| **IsolationForest** | Serving (on-demand) | `decision_function < 0` (7 cyclical features) | ~100ms |

`both_anomaly = true` when both agree → most reliable alert. Details: [ANOMALY.md](ANOMALY.md)

### RAG Pipeline (LLM Explain)

```
ChromaDB collections (3 context layers):
  ├─ anomaly_events    (500+ docs) — 7-day anomaly history from Postgres
  ├─ traffic_patterns  (3360 docs) — baseline (route × dow × hour) from gold
  └─ external_context  (192 docs)  — HCMC weather 7 days (Open-Meteo, rolling)

Indexing schedule:
  hourly-gold (every 1h)  → anomaly_events + external_context (weather)
  retrain (every 6h)      → traffic_patterns (full gold scan)

serving /explain & /rca:
  Query → OllamaEmbedder (nomic-embed-text, 768-dim)
         → ChromaDB: anomaly (route filter) + pattern (route filter) + weather (24h window)
         → Inject context → Ollama (qwen2.5:3b) → Stream SSE

serving /chat:
  Postgres snapshot (traffic) + Open-Meteo live fetch (cache 15 min)
         → Inject → Ollama → Stream SSE
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

platform/schemas/  # JSON Schema for Kafka message formats
```

---

## Services & Ports (Dev)

| Service | Port | Description |
|---------|------|-------------|
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

> **Dev**: Ollama runs natively on macOS (Metal GPU), not in a Docker container. Services point to `host.docker.internal:11434`.

---

## Quick Start (Dev)

### Prerequisites

- Docker Desktop (Mac) or Docker Engine + Compose (Linux)
- `uv` — Python package manager
- Ollama (native, macOS): `brew install ollama`

### 1. Clone

```bash
git clone git@github.com:tyri0n11/urban-pulse-platform.git
cd urban-pulse-platform
git clone git@github.com:tyri0n11/v0-urban-pulse-dashboard.git
```

> The UI repo must live inside `urban-pulse-platform/` — docker-compose context path is `../../v0-urban-pulse-dashboard`.

### 2. Create `.env`

```bash
cp .env.example .env
```

Fill in:

```env
VIETMAP_API_KEY=<your key>
MINIO_ROOT_USER=<username>
MINIO_ROOT_PASSWORD=<password, no @ symbol>
NESSIE_S3_ACCESS_KEY=<same as MINIO_ROOT_USER>
NESSIE_S3_SECRET_KEY=<same as MINIO_ROOT_PASSWORD>
POSTGRES_USER=urbanpulse
POSTGRES_PASSWORD=<password, no @ symbol>
```

> **Do not use `@` in passwords** — asyncpg parses the DSN and `@` breaks the host separator.

### 3. Pull Ollama models

```bash
ollama pull qwen2.5:3b
ollama pull nomic-embed-text
```

### 4. Start stack

```bash
make dev
```

Wait ~2 minutes for services to become healthy. Check:

```bash
make status
make logs
```

### 5. Bootstrap (first time)

```bash
make bootstrap   # Create Iceberg tables, run medallion pipeline from scratch
make train       # Train IsolationForest, push to MLflow
```

### 6. Initialize RAG index (first time)

```bash
# Index weather + anomaly events only (fast, ~60s)
docker exec batch-service .venv/bin/prefect deployment run rag-index/rag-index-deployment \
  --param index_patterns=false

# Full index including traffic patterns (slower ~5-10 min due to Ollama embedding)
docker exec batch-service .venv/bin/prefect deployment run rag-index/rag-index-deployment
```

### 7. Open UI

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
make dev            # Start full stack
make down           # Stop all containers
make status         # docker ps
make logs           # Tail logs

# Build
make build          # Rebuild all images
make build-serving  # Rebuild serving only
make build-ingestion

# ML
make train          # Trigger retrain manually
make bootstrap      # Full medallion rebuild from scratch

# Code quality
make lint           # ruff check
make typecheck      # mypy strict
make test           # pytest (unit + integration)
make test-unit
make test-integration

# Routes
python routes_generator.py  # Regenerate routes.json from zones.json
```

### Rebuild and restart a single service

```bash
docker compose --env-file .env -f infra/docker/docker-compose.dev.yaml build --no-cache serving
docker compose --env-file .env -f infra/docker/docker-compose.dev.yaml up -d serving
```

### Trigger a Prefect flow manually

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
GET  /online/routes                      — Latest + static coords from routes.json
GET  /online/lag                         — p50/p95/max ingest lag stats

GET  /anomalies/current                  — Anomalous routes right now
GET  /anomalies/history                  — Anomaly events, ?hours=24
GET  /anomalies/summary                  — Count per hour
GET  /anomalies/{route_id}               — Full timeline for one route
GET  /anomalies/{route_id}/explain       — SSE: LLM explanation (RAG-enhanced)

GET  /metrics/routes                     — Lightweight snapshot (all routes)
GET  /metrics/routes/{route_id}          — Per-hour trend
GET  /metrics/zones                      — Aggregate by zone
GET  /metrics/leaderboard                — Top N by |zscore|
GET  /metrics/heatmap                    — Route × hour zscore matrix

GET  /predict/anomalies                  — IForest score all routes
GET  /predict/anomalies/{route_id}       — IForest score one route
GET  /predict/model/info                 — Cache age, loaded URI, next refresh

POST /rca                                — RAG-powered Root Cause Analysis (SSE)
POST /rca/feedback                       — Thumbs up/down for fine-tuning
GET  /rca/logs                           — Recent interaction logs

POST /chat                               — Conversational AI with live snapshot (SSE)
GET  /chat/context                       — Raw snapshot (debug)

POST /telegram/webhook                   — Telegram bot webhook
GET  /telegram/info                      — Bot info (verify token)
GET  /telegram/set-webhook?url=...       — Register webhook URL

GET  /events/traffic                     — SSE: traffic snapshot every 15s

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

| Table | Layer | Description |
|-------|-------|-------------|
| `silver.traffic_route` | Silver | Cleaned observations from bronze |
| `gold.traffic_hourly` | Gold | Aggregates per (route_id, hour_utc) |
| `gold.traffic_baseline` | Gold | Stats per (route_id, dow, hour): mean, stddev for z-score |
| `online_route_features` | Postgres | Real-time Welford window + z-score |
| `route_iforest_scores` | Postgres | IForest scores every 15s (persistence, anomaly history) |
| `anomaly_alert_log` | Postgres | Telegram alert history (30-min dedup) |
| `rag_interaction_log` | Postgres | RCA query history for fine-tuning |

### Batch flows (Prefect)

| Flow | Schedule | Description |
|------|----------|-------------|
| `microbatch` | Every 5 min | Bronze → Silver (incremental) |
| `hourly-gold` | Every 1 hr | Silver → Gold + RAG index (anomaly events + weather from Open-Meteo) |
| `retrain` | Every 6 hrs | Gold → baseline + POST `/train` + RAG index (traffic patterns full scan) |
| `alert` | Every 5 min | Check anomalies → Telegram alert (30-min cooldown per route) |
| `rag-index` | Manual | Full RAG re-index; use `--param index_patterns=false` for weather + anomalies only |
| `bootstrap` | Manual | Full rebuild from scratch |
| `backfill` | Manual | Time-range backfill for silver |

---

## Route Format

```
zone{N}_{zone_name}_to_zone{M}_{zone_name}
Example: zone1_urban_core_to_zone4_southern_port
```

6 zones: `urban_core`, `eastern_innovation`, `northern_industrial`, `southern_port`, `western_periurban`, `southern_coastal`

20 routes (not all 30 pairs). `routes.json` is the source of truth — regenerate with `python routes_generator.py`.

---

## Toolchain

| Tool | Purpose |
|------|---------|
| `uv` | Python package manager, workspace |
| `hatchling` | Build backend |
| `ruff` | Linter (line-length 100, target py312) |
| `mypy --strict` | Type checker |
| `pytest` | Tests with `unit` and `integration` markers |
| `confluent_kafka` | Kafka producer/consumer |
| `pyiceberg` | Iceberg table read/write |
| `duckdb` | In-process analytics (feature engineering) |
| `mlflow` | Experiment tracking + model registry (`< 2.22`) |
| `chromadb` | Vector database (RAG) |
| `httpx` | Async HTTP (Ollama, Telegram API) |

---

## Key Design Decisions

**Why Welford instead of a buffer?**
O(1) memory regardless of throughput. Restart-safe because the window is reconstructed from `mean + stddev + count` stored in Postgres.

**Why per-route IForest instead of a global model?**
Each route has different characteristics (duration 15–90 min). A global model would be biased toward the most common routes.

**Why cyclical time encoding (sin/cos)?**
Day-of-week and hour-of-day are circular variables. Integer encoding (0–23, 0–6) creates artificial distance between the end and start of a cycle. Sin/cos preserves continuity: `hour=23` is close to `hour=0`.

**Why RAG instead of fine-tuning?**
Fine-tuning requires labeled data and compute. RAG injects real historical evidence into context immediately — no retraining needed, always up-to-date with the latest data.

**Why dual-signal instead of one?**
Z-score is fast and interpretable but only looks at one dimension (duration). IForest catches complex multi-dimensional patterns. `both_anomaly` reduces false positives.

**Why `mlflow < 2.22`?**
Server runs v2.21.3. Newer clients hit 404s on changed endpoints. Version pinned across the entire workspace.

---

## Docs

| File | Contents |
|------|---------|
| [ANOMALY.md](ANOMALY.md) | Dual-signal design, z-score, IForest cyclical features |
| [ML.md](ML.md) | Feature engineering detail, Welford, RAG pipeline |
| [SYSTEM.md](SYSTEM.md) | Infrastructure, ports, service URLs, schema tables |
| [DEPLOY.md](DEPLOY.md) | Production deployment (Traefik + TLS + RAG setup) |
| [CLAUDE.md](CLAUDE.md) | Guidance for AI assistant |

---

## Production

Deployed on homelab via Traefik + Let's Encrypt. Details: [DEPLOY.md](DEPLOY.md)

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
