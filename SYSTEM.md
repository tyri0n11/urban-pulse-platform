# Urban Pulse — System Overview

## What it is

Real-time traffic anomaly detection platform for Ho Chi Minh City. Polls VietMap API every 5 min, processes data through a medallion lakehouse, detects anomalies via dual-signal (z-score + IsolationForest), and streams results to a Next.js dashboard via SSE.

## Hardware

- **Homelab**: 1 machine, Intel i5-8th gen, 24 GB RAM
- **Domain**: `tyr1on.io.vn` — Cloudflare DNS (A records `@` and `*` → 171.248.102.63, DNS-only, no proxy)
- **Router**: Viettel ZTE F670Y — port forwarding 80 + 443 → 192.168.1.9
- **OS path**: `/opt/urban-pulse/` for all persistent volumes

---

## Repos

| Repo | Location | Purpose |
|------|----------|---------|
| `urban-pulse-platform` | `~/urban-pulse-platform` | Backend monorepo (all services) |
| `v0-urban-pulse-dashboard` | `~/v0-urban-pulse-dashboard` | **Active UI** (cloned inside platform repo as `v0-urban-pulse-dashboard/`) |
| `v0-urban-pulse-production-UI` | `~/v0-urban-pulse-production-UI` | Old new UI — not in use |

> The dashboard repo must live inside `urban-pulse-platform/` so docker-compose context path `../../v0-urban-pulse-dashboard` resolves correctly.

---

## Services

| Service | Image | Exposed (prod) | Purpose |
|---------|-------|---------------|---------|
| **traefik** | traefik:v3.3 | :80, :443 | Reverse proxy + Let's Encrypt TLS |
| **redpanda** | redpanda:v25.3.10 | internal | Kafka-compatible broker |
| **redpanda-console** | redpanda/console:v3.5.3 | `redpanda.tyr1on.io.vn` | Broker UI (basicauth) |
| **minio** | minio:latest | `minio.tyr1on.io.vn` | S3-compatible object store (AGPL) |
| **nessie** | projectnessie/nessie:0.99.0 | internal | Iceberg catalog |
| **dremio** | dremio/dremio-oss:latest | `dremio.tyr1on.io.vn` | SQL query engine |
| **postgres** | postgres:16-alpine | internal | Online feature store |
| **mlflow** | custom v2.21.3 | `mlflow.tyr1on.io.vn` | Experiment tracking + model registry (basicauth) |
| **prefect-server** | prefect:3-latest | `prefect.tyr1on.io.vn` | Workflow orchestration UI (basicauth) |
| **ingestion** | custom | internal | Polls VietMap API → Kafka |
| **streaming** | custom | internal | Kafka → MinIO bronze Parquet |
| **online** | custom | internal | Kafka → Postgres real-time features |
| **batch** | custom | internal | Prefect: bronze→silver→gold + retrain trigger |
| **ml** | custom | internal | FastAPI: IsolationForest train endpoint |
| **serving** | custom | `api.tyr1on.io.vn` | FastAPI REST + SSE endpoint |
| **ui** | custom (Next.js) | `tyr1on.io.vn` | Dashboard (v0-urban-pulse-dashboard) |
| **grafana** | grafana:11.6.0 | `grafana.tyr1on.io.vn` | Metrics + logs dashboards |
| **loki** | grafana/loki:3.4.2 | internal | Log aggregation |
| **promtail** | grafana/promtail:3.4.2 | internal | Log shipper |

---

## Data Flow

```
VietMap API (every 5 min)
  └─► ingestion ──► Kafka: traffic-route-bronze
                      ├─► streaming ──► MinIO bronze/year=Y/month=M/day=D/hour=H/*.parquet
                      └─► online    ──► Postgres: online_route_features

MinIO bronze
  └─► batch (5 min)  ──► Iceberg silver.traffic_route
  └─► batch (1 hr)   ──► Iceberg gold.traffic_summary
  └─► batch (6 hrs)  ──► POST ml-service:8000/train
                           └─► MLflow: traffic-anomaly-iforest@champion
                                 └─► serving loads on /predict (TTL 1h)

Postgres online_route_features
  └─► serving /events/traffic ──► SSE ──► UI dashboard (every 15s)
```

### Route IDs

Format: `zone{N}_{zone_name}_to_zone{M}_{zone_name}`
Example: `zone1_urban_core_to_zone4_southern_port`

6 zones: `urban_core`, `eastern_innovation`, `northern_industrial`, `southern_port`, `western_periurban`, `southern_coastal`
20 routes total (not all pairs — see `routes.json`).

---

## SSE Format (old UI expects this exactly)

```json
{
  "type": "traffic_update",
  "routes": [{ "route_id", "origin", "destination", "origin_anchor", "destination_anchor",
               "mean_duration_minutes", "duration_zscore", "is_anomaly", "updated_at", ... }],
  "anomalies": [...],
  "lag": { "active_routes", "p50_ms", "p95_ms", "max_ms", "mean_ms", "slo_met" },
  "ts": "ISO string"
}
```

`origin_anchor` / `destination_anchor` come from `routes.json` as `[lat, lng]` arrays.

---

## Anomaly Detection

Two independent signals merged in serving:

| Signal | Source | Threshold | Field |
|--------|--------|-----------|-------|
| Z-Score | online service, per-message | `\|z\| > 3.0` | `is_anomaly` |
| IsolationForest | serving, on-demand | model.predict == -1 | `iforest_anomaly` |

`both_anomaly = is_anomaly AND iforest_anomaly` — most reliable alert.

**Known data quality issue**: When `stddev_duration_minutes = 0` (too few observations or identical data from VietMap), `duration_zscore = NULL` → `is_anomaly = false`. Needs ≥2 windows with different durations for z-score to activate.

---

## Key Ports (dev)

| Service | Port |
|---------|------|
| Redpanda | 19092 |
| MinIO API | 9000, Console 9001 |
| MLflow | 5000 |
| Prefect | 4200 |
| Postgres | 5432 |
| ML service | 8000 |
| Serving API | 8001 |
| Grafana | 3001 |
| UI | 3000 |

---

## CORS

Serving allows: `localhost:3000`, `127.0.0.1:3000`, `https://tyr1on.io.vn`, `https://www.tyr1on.io.vn`

---

## MLflow constraint

`mlflow < 2.22` everywhere — server runs v2.21.3, newer clients hit 404s on changed endpoints.

---

## Online Feature Store Schema

`online_route_features` table (Postgres) — columns written by the `online` service:

| Column | Type | Description |
|--------|------|-------------|
| `route_id` | TEXT | PK |
| `window_start` | TIMESTAMPTZ | PK — current UTC hour boundary |
| `observation_count` | INTEGER | Messages received in window |
| `mean_duration_minutes` | DOUBLE | Welford running mean |
| `stddev_duration_minutes` | DOUBLE | Welford running stddev |
| `last_duration_minutes` | DOUBLE | Most recent observation |
| `mean_heavy_ratio` | DOUBLE | Running mean heavy vehicle ratio |
| `duration_zscore` | DOUBLE | `(mean - baseline.mean) / baseline.stddev`; NULL if no baseline |
| `is_anomaly` | BOOLEAN | `abs(zscore) > 3.0` |
| `last_ingest_lag_ms` | BIGINT | E2E Kafka→Postgres latency |
| `heavy_ratio_deviation` | DOUBLE | `mean_heavy_ratio - baseline.heavy_ratio_mean` |
| `p95_to_mean_ratio` | DOUBLE | `(mean + 2σ) / mean` — normal distribution p95 approx |
| `max_severe_segments` | DOUBLE | Max severe segments seen in window |

Last 3 columns were added to eliminate training/serving feature skew. Serving reads them directly — no proxy computation.

---

## IsolationForest Feature Vector

Feature order is fixed — must match `ml/features/traffic_features.py:FEATURE_COLUMNS`:

```python
["duration_zscore", "heavy_ratio_deviation", "p95_to_mean_ratio", "observation_count", "max_severe_segments"]
```

Training source: `gold.traffic_hourly` JOIN `gold.traffic_baseline` (DuckDB SQL).
Serving source: `online_route_features` columns (computed identically by online service).

---

## Kafka Consumer Config

| Service | `auto.offset.reset` | `enable.auto.commit` |
|---------|--------------------|--------------------|
| `streaming` | `earliest` | `False` (manual, after MinIO flush) |
| `online` | `earliest` | `False` (manual, after Postgres upsert) |

`online` was previously `latest` — changed to `earliest` so events are not lost on restart.

---

## Failure Handling

| Layer | Behavior |
|-------|----------|
| streaming → MinIO | Buffer only cleared after successful upload; Kafka offset committed after flush |
| online → Postgres | Kafka offset committed after upsert; `OperationalError` triggers reconnect + 1 retry |
| online parse error | Bad messages logged and skipped (Kafka offset still committed — no DLQ) |
| batch tasks | Prefect `retries=3, retry_delay_seconds=60` on all tasks |
| serving model load | Falls back to zscore-only if no MLflow model for route |

---

## Bootstrap / Operations Order

Fresh environment or after data wipe:
```
make dev          # start all services
make bootstrap    # exec into batch container: bronze→silver→gold→baseline
make train        # POST /train to ml-service
```

`make bootstrap` requires batch-service to be running (`make dev` first). It runs `bootstrap_cli` directly — does not go through Prefect scheduler.

Ongoing retrains happen automatically via Prefect `retrain` flow every 6h (runs `baseline_learning` then triggers `/train`).

### Why routes_skipped=20 in MLflow

`build_features()` does `LEFT JOIN gold × baseline WHERE b.route_id IS NOT NULL`. If `gold.traffic_baseline` is empty, all rows are filtered → 0 features → every route skips. Fix: run `make bootstrap` then `make train`.
