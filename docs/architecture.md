# Architecture

Urban Pulse is a near-real-time traffic anomaly detection system built as a Python monorepo. This document describes the system architecture from data ingestion through to the user-facing dashboard.

---

## High-Level Overview

```
External Sources                  Ingestion              Event Bus
─────────────────                 ─────────              ─────────
VietMap API (5 min poll) ───────► ingestion ───────────► Kafka topic: traffic-route-bronze
Open-Meteo API (weather) ──┐                                          │
                            │                                          ├─► streaming ──► MinIO (bronze Parquet)
                            │                                          └─► online ────► Postgres (online features)
                            │
                            └──► batch (hourly) ──► ChromaDB (weather history)
                            └──► serving /chat   ──► live fetch (cache 15 min)

MinIO (bronze Parquet)
  └─► batch/microbatch (5 min)  ──► Iceberg: silver.traffic_route
  └─► batch/hourly-gold (1 hr)  ──► Iceberg: gold.traffic_hourly
                                        └─► ChromaDB: anomaly_events + external_context
  └─► batch/retrain (6 hr)      ──► POST ml-service:8000/train
                                        └─► MLflow: traffic-anomaly-iforest@champion
                                        └─► ChromaDB: traffic_patterns
  └─► batch/alert (5 min)       ──► Telegram push alerts

Postgres (online_route_features)
  └─► serving /events/traffic (SSE) ──► IForest scoring ──► route_iforest_scores
  └─► serving /predict, /anomalies, /metrics ──► REST API
  └─► serving /explain, /rca, /chat ──► RAG + Ollama ──► SSE stream

Next.js Dashboard ─────────────────────────────────────────────────────────►  User
```

---

## Monorepo Layout

```
apps/
  ingestion/    # Polls VietMap API every 5 min → publishes TrafficRouteObservation to Kafka
  streaming/    # Kafka consumer → Parquet files on MinIO (bronze layer)
  batch/        # Prefect flows: medallion pipeline, ML retrain, RAG indexer, alerter
  ml/           # FastAPI: receives POST /train → fits IForest per route → logs to MLflow
  online/       # Kafka consumer → Welford z-score → upserts online_route_features in Postgres
  serving/      # FastAPI REST + SSE: reads Postgres + MLflow → predictions, explanations, chat

packages/
  core/              # Pydantic domain models + Settings
  infra-clients/     # Kafka, MinIO, DuckDB, PyIceberg, MLflow client factories
  observability/     # Logging utilities

infra/
  docker/            # docker-compose base + dev overlay
  monitoring/        # Prometheus, Grafana (dashboards), Loki, Promtail

platform/schemas/    # JSON Schema for Kafka message format validation
```

---

## Service Descriptions

### ingestion

Polls the VietMap Traffic API every 5 minutes for all 20 monitored routes. Each poll produces one `TrafficRouteObservation` Pydantic model per route, serialized to JSON, and published to the `traffic-route-bronze` Kafka topic with an `ingest_ts` header (Unix timestamp of the API call). This header is the anchor for all downstream latency measurements.

### streaming

Confluent Kafka consumer that reads `traffic-route-bronze` and writes Parquet files to MinIO under:
```
urban-pulse/bronze/traffic-route-bronze/year=Y/month=M/day=D/hour=H/<uuid>.parquet
```
Uses manual offset commits (commits only after successful MinIO flush). Buffers 500 messages or flushes every 30 seconds. Failed messages go to `traffic-route-bronze-dlq`.

### online

Second Kafka consumer of the same topic. For each incoming message it:
1. Updates a per-route `RouteWindow` using Welford's online algorithm (running mean + M₂ accumulator)
2. Computes z-score against the cached gold baseline for the current `(route_id, day_of_week, hour_of_day)` slot
3. Upserts into `online_route_features` — the real-time feature store

Baseline is loaded from `gold.traffic_baseline` at startup and refreshed every 6 hours. Window state is reconstructed from Postgres on restart (`_restore_windows()`).

### batch

Prefect orchestration service running five scheduled flows and several manual-trigger flows:

| Flow | Schedule | Description |
|------|----------|-------------|
| `microbatch` | Every 5 min | Reads new bronze Parquet files → writes to `silver.traffic_route` (incremental) |
| `hourly-gold` | Every 1 hr | Aggregates silver → `gold.traffic_hourly`; indexes anomaly events + weather into ChromaDB |
| `retrain` | Every 6 hr | Full gold scan → POST /train → ChromaDB traffic_patterns full re-index |
| `alert` | Every 5 min | Reads active IForest-confirmed anomalies → Telegram push (30-min cooldown per route) |
| `bootstrap` | Manual | Full medallion rebuild from scratch |
| `backfill` | Manual | Time-range silver backfill |
| `rag-index` | Manual | Re-index RAG; `index_patterns=false` for weather+anomalies only (~60s) |

### ml

Lightweight FastAPI service that exposes `POST /train`. When called by the `retrain` Prefect flow, it:
1. Reads `gold.traffic_hourly` via DuckDB
2. Builds the 7-feature matrix per route (traffic ratios + cyclical time encoding)
3. Fits a per-route `IsolationForest(contamination=0.05, n_estimators=100)`
4. Logs model artifact to MLflow and promotes to `@champion` alias

### serving

Main user-facing API (FastAPI + Uvicorn). Responsibilities:
- **Feature store reads** (`/online/*`) — latest and historical snapshots from Postgres
- **Anomaly queries** (`/anomalies/*`) — current + history + summary, merging z-score and IForest signals
- **Metrics** (`/metrics/*`) — zone aggregations, leaderboard, heatmap
- **Predictions** (`/predict/*`) — on-demand IForest scoring with 1h model cache
- **LLM endpoints** (`/explain`, `/rca`, `/chat`) — RAG retrieval + Ollama streaming
- **SSE** (`/events/traffic`) — 15s push to dashboard; also drives background IForest scorer loop
- **Telegram webhook** (`/telegram/webhook`) — inbound messages from @tyr1on_system_alert_bot

A background `asyncio.Task` (`_iforest_scorer_loop`) scores all routes every 15 seconds and writes results to `route_iforest_scores`, independent of whether any frontend is connected.

---

## Data Layer Detail

### Kafka

Topic: `traffic-route-bronze`. Single partition per topic (homelab scale). Message format: JSON. Key: `route_id`. Header: `ingest_ts` (Unix float seconds).

### MinIO

Bucket layout:
```
urban-pulse/
  bronze/traffic-route-bronze/year=Y/month=M/day=D/hour=H/*.parquet
  silver/  (managed by PyIceberg)
  gold/    (managed by PyIceberg)
mlflow/    (MLflow artifact store)
warehouse/ (Dremio warehouse)
```

### Iceberg Tables (via Project Nessie)

| Table | Layer | Key columns |
|-------|-------|-------------|
| `silver.traffic_route` | Silver | `route_id`, `event_ts`, `duration_minutes`, `congestion_*` |
| `gold.traffic_hourly` | Gold | `route_id`, `hour_utc`, `avg_duration_minutes`, `avg_heavy_ratio`, `max_severe_segments` |
| `gold.traffic_baseline` | Gold | `route_id`, `day_of_week`, `hour_of_day`, `baseline_duration_mean`, `baseline_duration_stddev` |
| `gold.weather_hourly` | Gold | `hour_utc`, `temperature_2m`, `precipitation`, `weather_code` |

### PostgreSQL (Online Feature Store)

Primary tables:

**`online_route_features`** — real-time Welford window output, upserted by `online` service:
```
route_id, window_start (PK) — current UTC hour bucket
mean_duration_minutes, stddev_duration_minutes — Welford running stats
mean_heavy_ratio, mean_moderate_ratio, max_severe_segments — congestion metrics
duration_zscore — (mean_duration - baseline_mean) / baseline_stddev
is_anomaly — zscore > threshold (one-sided)
last_ingest_lag_ms — ingest_ts header to Postgres write latency
updated_at — precise timestamp of last write
```

**`route_iforest_scores`** — IForest scoring results, written every 15s by serving:
```
route_id, window_start (PK)
iforest_score, iforest_anomaly, both_anomaly — latest single-shot result
score_count, anomaly_count, both_count — aggregation for majority vote
```

**`prediction_history`** — full E2E latency log, one row per 15s scoring cycle:
```
scored_at, route_id, window_start
iforest_score, iforest_anomaly, zscore_anomaly, both_anomaly
duration_zscore, mean_duration_minutes, mean_heavy_ratio
ingest_lag_ms — poll → Postgres write (system: VietMap + Kafka + online service)
staleness_ms — Postgres write → IForest score (data age at scoring time)
scoring_ms — IForest predict() wall clock
full_e2e_ms — ingest_lag_ms + staleness_ms + scoring_ms
```

**`rag_interaction_log`** — audit log for all LLM interactions (explain, rca, chat)

**`anomaly_alert_log`** — Telegram alert history (deduplication with 30-min cooldown)

### ChromaDB (Vector Store)

Three collections, all using `nomic-embed-text` (768-dim) with cosine similarity:

| Collection | Size | Metadata filters | Refresh |
|-----------|------|-----------------|---------|
| `anomaly_events` | ~500 docs | `route_id`, `hour`, `dow` | Every 1h |
| `traffic_patterns` | ~3360 docs | `route_id`, `hour`, `dow` | Every 6h |
| `external_context` | ~192 docs | `hour_ts`, `is_rain`, `is_storm` | Every 1h |

---

## End-to-End Latency Model

The system tracks two distinct SLOs, stored per-tick in `prediction_history`:

### SLO 1 — Pipeline Processing Latency

Measures **system overhead** only — time added by Urban Pulse's own components.

```
ingest_lag_ms = time from VietMap API poll (ingest_ts header) to Postgres write
scoring_ms    = IsolationForest predict() wall clock

pipeline_latency = ingest_lag_ms + scoring_ms
```

Target: **p95 < 60 s**. This is fully within the system's control (Kafka + online service processing).

### SLO 2 — Data Freshness

Measures **data age at scoring time** — dominated by the external VietMap poll interval.

```
staleness_ms = scoring_ts - updated_at   (age of the Postgres row when IForest runs)
```

Target: **p95 < 310 s** (5-minute poll interval + 10 s buffer). The poll interval is an external constraint; this SLO validates the system is not adding significant additional delay.

### Separation rationale

Full E2E (`full_e2e_ms = ingest_lag_ms + staleness_ms + scoring_ms`) conflates system overhead with external constraints. A full_e2e of 5+ minutes is expected and correct when staleness dominates — it reflects the VietMap API poll frequency, not a system fault. Reporting them separately makes the thesis evaluation honest and actionable.

---

## Deployment

### Development

```bash
make dev         # docker-compose up all services
make bootstrap   # run full medallion pipeline from scratch
make train       # POST /train → MLflow
```

Ollama runs natively on macOS (Metal GPU). All other services run in Docker. Services reference Ollama at `host.docker.internal:11434`.

### Production

Single homelab machine (Intel i5-8th gen, 24 GB RAM). All services including Ollama run in Docker with NVIDIA GPU reservation. TLS via Traefik + Let's Encrypt ACME. DNS via Cloudflare with a custom DDNS service keeping A records current.

Endpoints:
- `tyr1on.io.vn` — Next.js dashboard
- `api.tyr1on.io.vn` — Serving API
- `mlflow.tyr1on.io.vn`, `prefect.tyr1on.io.vn`, `grafana.tyr1on.io.vn` — Ops UI (basic auth)
- `redpanda.tyr1on.io.vn`, `minio.tyr1on.io.vn`, `dremio.tyr1on.io.vn` — Data plane UI
