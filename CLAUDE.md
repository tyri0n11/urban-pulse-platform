# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Start the full dev stack
make dev

# Stop all containers
make down

# Status, logs
make status
make logs

# Lint (ruff), type-check (mypy strict)
make lint
make typecheck

# Tests
make test
make test-unit
make test-integration
uv run pytest apps/ingestion/tests/test_orchestrator.py  # single file

# Full medallion bootstrap (run after first make dev)
make bootstrap

# Trigger ML training manually
make train

# Rebuild specific images
make build-serving
make build-ingestion
make build            # all images

# Regenerate routes.json from zones.json
python routes_generator.py
```

All Python commands must be prefixed with `uv run`. Do not use `pip` or `python` directly.

To rebuild and restart a single service after code changes:
```bash
docker compose --env-file .env -f infra/docker/docker-compose.dev.yaml build --no-cache serving
docker compose --env-file .env -f infra/docker/docker-compose.dev.yaml up -d serving
```

To trigger a Prefect flow manually:
```bash
docker exec batch-service .venv/bin/prefect deployment run microbatch/microbatch-deployment
docker exec batch-service .venv/bin/prefect deployment run retrain/retrain-deployment
```

## Architecture

Python monorepo (uv workspace) for a real-time urban traffic monitoring platform for Ho Chi Minh City.

### Monorepo layout

```
apps/
  ingestion/   # Polls VietMap API every 5 min, publishes to Kafka
  streaming/   # Kafka consumer → Parquet files on MinIO (bronze layer)
  batch/       # Prefect flows: medallion pipeline + ML retrain trigger
  ml/          # FastAPI service: trains IsolationForest, logs to MLflow
  online/      # Consumes Kafka, writes real-time features to Postgres
  serving/     # FastAPI REST API: reads Postgres + MLflow for predictions

packages/
  core/            # Pydantic domain models (TrafficRouteObservation, etc.) + Settings
  infra-clients/   # Client factories: Kafka, MinIO, DuckDB, PyIceberg, MLflow
  observability/   # Logging utilities

infra/
  docker/          # docker-compose.base.yaml + dev overlay
  monitoring/      # Prometheus, Grafana dashboards, Loki, Promtail

platform/schemas/  # JSON Schema for Kafka message formats
```

### Complete data flow

```
VietMap API (every 5 min)
  └─► ingestion ──► Kafka: traffic-route-bronze
                      └─► streaming ──► MinIO: bronze/traffic-route-bronze/year=Y/month=M/day=D/hour=H/*.parquet
                      └─► online    ──► Postgres: online_route_features (real-time window features)
                                          └─► serving (reads on-demand)

MinIO bronze
  └─► batch (every 5 min) ──► Iceberg: silver.traffic_route
  └─► batch (every 1 hr)  ──► Iceberg: gold.traffic_summary
  └─► batch (every 6 hrs) ──► POST http://ml-service:8000/train
                                └─► MLflow registry: traffic-anomaly-iforest@champion
                                      └─► serving (loads on first /predict call, TTL 1h)
```

### Key infrastructure ports

| Service | Port | Notes |
|---|---|---|
| Redpanda (Kafka) | 19092 | External; internal: `redpanda:9092` |
| Redpanda Console | 8080 | |
| MinIO API | 9000 | |
| MinIO Console | 9001 | |
| Nessie (Iceberg catalog) | 19120 | |
| Dremio | 9047 | |
| MLflow | 5000 | |
| Prefect Server | 4200 | |
| PostgreSQL | 5432 | Online feature store |
| ML service | 8000 | FastAPI: /train, /health, /status |
| Serving API | 8001 | FastAPI: /online, /anomalies, /metrics, /predict |
| Loki | 3100 | |
| Grafana | 3001 | Changed from default 3000 to avoid conflict with Next.js UI |
| Next.js UI | 3000 | Lives in `../v0-urban-pulse-dashboard` (sibling repo) |

### Dual-signal anomaly detection

The platform detects anomalies using two independent signals:
- **Z-Score** (real-time, speed layer): computed by the `online` service per-message as events arrive from Kafka. Threshold: `|zscore| > 3.0`. Result stored in `online_route_features.is_anomaly`.
- **IsolationForest** (batch model, serving layer): trained every 6h on gold-layer data, registered in MLflow as `traffic-anomaly-iforest@champion`. Loaded by the serving API with a 1-hour TTL cache; scored on-demand against the latest `online_route_features` snapshot.

The `serving` app merges both signals in `/predict/anomalies` and `/anomalies/current`:
- `both_anomaly = True` = most reliable alert
- `zscore_anomaly` only = real-time signal
- `iforest_anomaly` only = ML signal

### MinIO bucket structure

```
urban-pulse/
  bronze/traffic-route-bronze/year=Y/month=M/day=D/hour=H/*.parquet  ← streaming writes
  silver/  (managed by PyIceberg, metadata in Nessie)
  gold/    (managed by PyIceberg, metadata in Nessie)
mlflow/    ← MLflow artifact store (model.pkl, metrics, etc.)
warehouse/ ← Dremio warehouse
```

### Batch pipeline flows (Prefect)

All flows defined in `apps/batch/src/batch/pipeline.py` and registered in `main.py` via `serve()`:

| Flow | Schedule | What it does |
|---|---|---|
| `microbatch` | every 5 min | Bronze Parquet → Silver Iceberg (incremental) |
| `hourly-gold` | every 1 hr | Silver Iceberg → Gold Iceberg (hourly aggregates) |
| `retrain` | every 6 hrs | Gold → baseline table, then POST /train to ml-service |
| `bootstrap` | manual | Full medallion rebuild from scratch |
| `backfill` | manual | Time-range backfill for silver |

`log_prints=True` is set on all flows — use `print()` or standard Python logging inside tasks.

**Critical**: Prefect runs flows in subprocesses. Logger configuration in `main.py` is NOT inherited. The `batch/__init__.py` adds explicit `StreamHandler` to all batch module loggers so they appear in container stdout (→ Promtail → Loki → Grafana).

### Serving API endpoint map

All endpoints read from Postgres (`online_route_features` table) via asyncpg connection pool.

```
/online/features                          # latest snapshot per route
/online/features/{route_id}              # single route latest
/online/features/{route_id}/history      # per-hour windows, ?hours=24
/online/routes                           # latest + static coords from routes.json
/online/lag                              # p50/p95/max ingest lag stats
/online/reconcile                        # online vs batch baseline comparison

/anomalies/current                       # anomalous routes now (runs IForest scoring)
/anomalies/history                       # anomaly events, ?hours=24
/anomalies/summary                       # count per hour for chart
/anomalies/{route_id}                    # full timeline for one route

/metrics/routes                          # lightweight snapshot (all routes)
/metrics/routes/{route_id}              # per-hour trend
/metrics/zones                           # aggregate by origin zone
/metrics/leaderboard                     # top N by |zscore|
/metrics/heatmap                         # route × hour zscore matrix

/predict/anomalies                       # IForest score all routes
/predict/anomalies/{route_id}           # IForest score one route
/predict/model/info                      # cache age, loaded uri, next refresh

/health/live
/health/ready
```

CORS is configured for `http://localhost:3000` only (the Next.js dev server).

### MLflow model loading in serving

`serving/services/prediction_service.py` loads the model using `mlflow.artifacts.download_artifacts` + `joblib` (avoids the implicit pandas dependency of `mlflow.sklearn.load_model`). It tries `@champion` alias first, falls back to `latest` version. The model is cached in-process with a 1-hour TTL.

Required env vars on the serving container:
- `MLFLOW_TRACKING_URI=http://mlflow:5000`
- `MLFLOW_S3_ENDPOINT_URL=http://minio:9000` (boto3 needs this for artifact download)
- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (MinIO credentials)

### Streaming service

`apps/streaming` uses `confluent_kafka` (not `kafka-python`). The `TrafficProcessor`:
- Buffers 500 messages or flushes every 30 seconds (whichever comes first)
- Uses manual offset commits — commits only after a successful MinIO flush
- Failed messages go to a dead-letter queue topic (`{topic}-dlq`)
- Computes end-to-end latency from the `ingest_ts` Kafka header

### Route ID format

Route IDs follow the pattern: `zone{N}_{zone_name}_to_zone{M}_{zone_name}`
Example: `zone1_urban_core_to_zone4_southern_port`

There are 6 zones (1–6): urban_core, eastern_innovation, northern_industrial, southern_port, western_periurban, southern_coastal. `routes.json` is the source of truth, generated from `zones.json` via `routes_generator.py`.

### Dependency rules

- All apps depend on `packages/core`.
- Infrastructure-touching apps use `packages/infra-clients`.
- Packages do not depend on each other.
- `mlflow` version must be `<2.22` everywhere (server runs v2.21.3; newer clients hit 404s on changed endpoints).

### Toolchain

- **Package manager**: `uv` workspace
- **Build backend**: `hatchling`
- **Linter**: `ruff` (line-length 100, target py312)
- **Type checker**: `mypy` (strict)
- **Test runner**: `pytest` with `unit` and `integration` markers
- **Python**: ≥ 3.12
