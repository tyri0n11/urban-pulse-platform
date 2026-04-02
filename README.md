# Urban Pulse Platform

Real-time traffic anomaly detection platform cho Thành phố Hồ Chí Minh. Polls VietMap API mỗi 5 phút, xử lý qua medallion lakehouse, phát hiện bất thường bằng dual-signal (Z-Score + IsolationForest), stream kết quả lên dashboard Next.js qua SSE.

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
  └─► batch (mỗi 1 giờ)  ──► Iceberg: gold.traffic_hourly + gold.traffic_summary
  └─► batch (mỗi 6 giờ)  ──► gold.traffic_baseline + POST /train → MLflow: iforest@champion

Postgres online_route_features
  └─► serving REST API + SSE ──► UI Dashboard (Next.js :3000)
```

### Dual-signal anomaly detection

| Signal | Tầng | Threshold | Latency |
|--------|------|-----------|---------|
| **Z-Score** | Online (Kafka consumer) | `\|z\| > 3.0` vs baseline lịch sử | < 20s |
| **IsolationForest** | Serving (on-demand) | `predict == -1` (5 features) | ~100ms |

`both_anomaly = true` khi cả hai đồng ý → cảnh báo tin cậy nhất. Chi tiết: [ANOMALY.md](ANOMALY.md)

---

## Monorepo Layout

```
apps/
  ingestion/   # Poll VietMap API → Kafka
  streaming/   # Kafka → MinIO bronze Parquet
  batch/       # Prefect: medallion pipeline + retrain trigger
  ml/          # FastAPI: train IsolationForest, log MLflow
  online/      # Kafka → Postgres real-time features (Welford + z-score)
  serving/     # FastAPI REST + SSE: /online, /anomalies, /metrics, /predict

packages/
  core/            # Pydantic models + Settings
  infra-clients/   # Kafka, MinIO, DuckDB, PyIceberg, MLflow client factories
  observability/   # Logging utilities

infra/
  docker/          # docker-compose.base + dev + prod overlays
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
| Serving API | 8001 | FastAPI: REST + SSE |
| Grafana | 3001 | Metrics + logs |
| Loki | 3100 | Log aggregation |
| Ollama | 11434 | LLM inference (qwen2.5:3b) |
| Next.js UI | 3000 | Dashboard (sibling repo) |

---

## Quick Start (Dev)

### Prerequisites

- Docker Desktop (Mac) hoặc Docker Engine + Compose (Linux)
- `uv` — Python package manager
- Git

### 1. Clone

```bash
git clone git@github.com:tyri0n11/urban-pulse-platform.git
cd urban-pulse-platform
git clone git@github.com:tyri0n11/v0-urban-pulse-dashboard.git
```

> UI repo phải nằm trong `urban-pulse-platform/` — docker-compose context path `../../v0-urban-pulse-dashboard` relative to `infra/docker/`.

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

### 3. Start stack

```bash
make dev
```

Đợi ~2 phút để các services healthy. Kiểm tra:

```bash
make status
make logs
```

### 4. Bootstrap (lần đầu)

```bash
make bootstrap   # Tạo Iceberg tables, chạy medallion pipeline từ đầu
make train       # Train IsolationForest, push lên MLflow
```

### 5. Mở UI

```
http://localhost:3000        — Dashboard
http://localhost:8001/docs   — Serving API docs
http://localhost:5000        — MLflow
http://localhost:4200        — Prefect
http://localhost:9001        — MinIO Console
http://localhost:3001        — Grafana
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

# Code quality (chạy trong uv workspace)
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
```

---

## Serving API Endpoints

Base URL (dev): `http://localhost:8001`

```
GET /online/features                    — Latest snapshot per route
GET /online/features/{route_id}         — Single route latest
GET /online/features/{route_id}/history — Per-hour windows, ?hours=24
GET /online/routes                      — Latest + static coords từ routes.json
GET /online/lag                         — p50/p95/max ingest lag stats
GET /online/reconcile                   — Online vs batch baseline comparison

GET /anomalies/current                  — Anomalous routes ngay lúc này (IForest)
GET /anomalies/history                  — Anomaly events, ?hours=24
GET /anomalies/summary                  — Count per hour
GET /anomalies/{route_id}               — Full timeline 1 route
GET /anomalies/{route_id}/explain       — SSE: LLM explanation từ Ollama

GET /metrics/routes                     — Lightweight snapshot (all routes)
GET /metrics/routes/{route_id}          — Per-hour trend
GET /metrics/zones                      — Aggregate by zone
GET /metrics/leaderboard                — Top N by |zscore|
GET /metrics/heatmap                    — Route × hour zscore matrix

GET /predict/anomalies                  — IForest score all routes
GET /predict/anomalies/{route_id}       — IForest score 1 route
GET /predict/model/info                 — Cache age, loaded URI, next refresh

GET /events/traffic                     — SSE: traffic snapshot mỗi 15s

GET /health/live
GET /health/ready
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
| `gold.traffic_hourly` | Gold | Aggregates per (route_id, hour_utc): avg_duration, p95, obs_count |
| `gold.traffic_baseline` | Gold | Stats per (route_id, dow, hour): mean, stddev dùng cho z-score |
| `gold.traffic_summary` | Gold | Zone-level aggregates |
| `online_route_features` | Postgres | Real-time Welford window + z-score, updated per Kafka message |

### Batch flows (Prefect)

| Flow | Schedule | Mô tả |
|------|----------|-------|
| `microbatch` | Mỗi 5 phút | Bronze → Silver (incremental) |
| `hourly-gold` | Mỗi 1 giờ | Silver → Gold hourly aggregates |
| `retrain` | Mỗi 6 giờ | Gold → baseline + POST `/train` |
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
| `confluent_kafka` | Kafka producer/consumer (không dùng kafka-python) |
| `pyiceberg` | Iceberg table read/write |
| `duckdb` | In-process analytics (feature engineering) |
| `mlflow` | Experiment tracking + model registry (`< 2.22`) |

---

## Key Design Decisions

**Tại sao Welford thay vì buffer?**
O(1) memory bất kể throughput. Restart-safe vì có thể reconstruct từ `mean + stddev + count` đã lưu trong Postgres.

**Tại sao IForest per-route thay vì global?**
Mỗi route có đặc trưng khác nhau (duration 15–90 phút). Global model sẽ bias về routes phổ biến.

**Tại sao dual-signal thay vì chỉ 1?**
Z-score nhanh và interpretable nhưng chỉ nhìn 1 chiều. IForest bắt được pattern phức tạp nhưng không có baseline rõ ràng. `both_anomaly` giảm false positive.

**Tại sao `confluent_kafka` thay vì `kafka-python`?**
`kafka-python` có compatibility issues với Redpanda. `confluent_kafka` (librdkafka) proven trên toàn platform.

**Tại sao `mlflow < 2.22`?**
Server chạy v2.21.3. Client mới hơn hit 404 trên changed endpoints. Pin version toàn workspace.

---

## Docs

| File | Nội dung |
|------|---------|
| [ANOMALY.md](ANOMALY.md) | Dual-signal design, z-score, IForest, known issues |
| [ML.md](ML.md) | Feature engineering chi tiết, Welford, baseline SQL |
| [SYSTEM.md](SYSTEM.md) | Infrastructure, ports, service URLs, schema tables |
| [DEPLOY.md](DEPLOY.md) | Production deployment lên homelab (Traefik + TLS) |
| [CLAUDE.md](CLAUDE.md) | Hướng dẫn cho AI assistant (commands, architecture) |

---

## Production

Deploy lên homelab qua Traefik + Let's Encrypt. Chi tiết: [DEPLOY.md](DEPLOY.md)

| URL | Service |
|-----|---------|
| `https://tyr1on.io.vn` | Dashboard |
| `https://api.tyr1on.io.vn` | Serving API |
| `https://grafana.tyr1on.io.vn` | Grafana |
| `https://mlflow.tyr1on.io.vn` | MLflow |
| `https://prefect.tyr1on.io.vn` | Prefect |
| `https://minio.tyr1on.io.vn` | MinIO |
| `https://redpanda.tyr1on.io.vn` | Redpanda Console |
| `https://dremio.tyr1on.io.vn` | Dremio |
