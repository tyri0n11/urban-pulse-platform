# Tech Stack

Complete technology inventory for Urban Pulse. Every component was selected to satisfy at least one of: open-source availability, production-grade reliability, and academic reproducibility on commodity hardware.

---

## Data Ingestion

| Component | Technology | Version | Role |
|-----------|-----------|---------|------|
| Traffic source | VietMap Traffic API | REST/JSON | Real-time congestion data for HCMC |
| Weather source | Open-Meteo API | REST/JSON | Free, no-key HCMC hourly weather |
| Ingestion service | Python (FastAPI worker) | 3.12 | Polls VietMap every 5 min, publishes to Kafka |
| Message broker | Apache Kafka (Redpanda) | v25.3.10 | Durable, partitioned event log |
| Schema validation | JSON Schema + Pydantic | — | Contract enforcement on Kafka messages |

**Why Redpanda over Apache Kafka?** Redpanda is Kafka-compatible (same client API) but ships as a single binary with no JVM dependency — significantly easier to run on homelab hardware. Drop-in replacement for the Kafka client library (`confluent_kafka`).

---

## Stream Processing

| Component | Technology | Version | Role |
|-----------|-----------|---------|------|
| Stream consumer | Python (`confluent_kafka`) | — | Kafka → MinIO bronze Parquet |
| Online features | Python (asyncpg + asyncio) | — | Kafka → Postgres (Welford z-score) |
| File format | Apache Parquet | — | Columnar, compressed, schema-encoded |
| Object store | MinIO | latest | S3-compatible, stores all Parquet files |

**Online feature store**: PostgreSQL is used as the online feature store rather than Redis because the workload is write-once-per-route (upsert keyed on `route_id, window_start`), and reads are low-frequency (15s SSE poll). PostgreSQL's JSONB + asyncpg connection pool is sufficient and avoids an extra infrastructure dependency.

---

## Batch / Lakehouse

| Component | Technology | Version | Role |
|-----------|-----------|---------|------|
| Orchestration | Prefect | 3.x | Flow scheduling + observability |
| Query engine (batch) | DuckDB | 0.10+ | In-process SQL over Parquet + Iceberg |
| Table format | Apache Iceberg | 1.5+ | ACID table format for Silver + Gold layers |
| Iceberg catalog | Project Nessie | 0.99.0 | Git-like version control for Iceberg tables |
| SQL query engine | Dremio (OSS) | latest | Ad-hoc queries over Iceberg via Arrow Flight |
| Baseline learning | DuckDB SQL | — | Aggregates gold layer → `gold.traffic_baseline` |
| Weather pipeline | DuckDB + PyIceberg | — | Open-Meteo → Silver → Gold (hourly weather) |

**Medallion layers:**
- **Bronze**: Raw Parquet from streaming (schema-exact copy of Kafka messages)
- **Silver**: Cleaned, typed, deduplicated Iceberg table (`silver.traffic_route`)
- **Gold**: Hourly aggregations (`gold.traffic_hourly`), baselines (`gold.traffic_baseline`), weather (`gold.weather_hourly`)

---

## Machine Learning

| Component | Technology | Version | Role |
|-----------|-----------|---------|------|
| Anomaly model | scikit-learn IsolationForest | 1.4+ | Unsupervised anomaly detection on 7 features |
| Feature engineering | DuckDB SQL + Python | — | Cyclical time encoding (sin/cos) |
| Experiment tracking | MLflow | 2.21.3 | Model registry + artifact store |
| Artifact store | MinIO (`mlflow/` bucket) | — | `model.pkl` + run metrics |
| Model serving | joblib (`model.pkl`) | — | Loaded in-process by serving API |
| Retraining trigger | Prefect `retrain` flow | every 6h | Gold → POST ml-service:8000/train |

**Version pin**: `mlflow < 2.22` everywhere — server runs 2.21.3; newer client libraries hit 404s on changed artifact endpoints.

**IsolationForest design choices:**
- `contamination=0.05` — assumes 5% of historical data contains genuine anomalies
- `n_estimators=100`, `random_state=42` — reproducible
- Per-route model — each of the 20 routes gets its own forest to capture route-specific normal behaviour
- 7-feature vector with cyclical time encoding avoids the artificial discontinuity between hour 23 → 0 that integer encoding creates

---

## LLM & RAG

| Component | Technology | Version | Role |
|-----------|-----------|---------|------|
| LLM runtime | Ollama | latest | Local model inference (no API key) |
| LLM model | qwen2.5:3b | 3B params | Chat, explain, RCA, Telegram alerts |
| Embedding model | nomic-embed-text | 768-dim | Semantic search for RAG |
| Vector database | ChromaDB | latest | 3 collections: anomaly_events, traffic_patterns, external_context |
| HTTP client | httpx (async) | — | Streaming calls to Ollama API |

**ChromaDB collections:**

| Collection | Documents | Refresh | Purpose |
|-----------|-----------|---------|---------|
| `anomaly_events` | ~500 | Every 1h | HCMC anomaly history (7 days) from Postgres |
| `traffic_patterns` | ~3360 | Every 6h | Route × hour × DOW baseline from gold layer |
| `external_context` | ~192 | Every 1h | HCMC weather history (7 days) from Open-Meteo |

**RAG retrieval strategy**: For `/explain` and `/rca`, the system performs three parallel lookups — live weather (direct API call, cached 15 min), recent traffic patterns (ChromaDB filter by `route_id`), and recent weather chunks (ChromaDB filter by `hour_ts >= now - 24h`). All context is injected into the prompt before streaming to Ollama.

**Chat session context**: `/chat` uses Ollama `/api/chat` (multi-turn messages array) rather than `/api/generate` (stateless). The frontend sends the last 10 messages as history; the backend caps this to prevent context overflow in qwen2.5:3b's 32k token window.

---

## Serving API

| Component | Technology | Version | Role |
|-----------|-----------|---------|------|
| Framework | FastAPI | 0.111+ | REST API + SSE |
| ASGI server | Uvicorn | — | Async HTTP server |
| DB client | asyncpg | — | Async PostgreSQL connection pool |
| SSE | FastAPI `StreamingResponse` | — | `/events/traffic` — 15s push to UI |
| Model cache | In-process dict + TTL | 1h | Per-route IForest models |
| CORS | FastAPI middleware | — | localhost:3000, tyr1on.io.vn |

**Background scorer loop**: A `asyncio.Task` runs `_iforest_scorer_loop()` every 15 seconds independently of SSE connections. This ensures `route_iforest_scores` has data even when no frontend is connected, enabling `/anomalies/history` and `/anomalies/summary` to return IForest-enriched results for past hours.

---

## Frontend

| Component | Technology | Version | Role |
|-----------|-----------|---------|------|
| Framework | Next.js (App Router) | 14+ | React SSR + client components |
| Styling | Tailwind CSS | 3+ | Utility-first CSS |
| Charts | Recharts | — | Anomaly timeline, trend charts, heatmap |
| Map | Leaflet + OpenStreetMap | — | Interactive route map with anomaly overlay |
| State | SWR | — | Data fetching + auto-revalidation |
| Chat | Custom streaming hook | — | SSE consumer for chat responses |
| Icons | Lucide React | — | UI icons |

---

## Infrastructure & Operations

| Component | Technology | Version | Role |
|-----------|-----------|---------|------|
| Container runtime | Docker Compose | v2 | Local dev + prod orchestration |
| Reverse proxy | Traefik | v2.11 | TLS termination + routing |
| TLS | Let's Encrypt (ACME) | — | Automatic cert provisioning |
| DNS | Cloudflare | — | A records → homelab IP (171.248.102.63) |
| Observability | Prometheus + Grafana | 11.6.0 | Metrics dashboards |
| Log aggregation | Loki + Promtail | 3.4.2 | Structured log ingestion from Docker |
| Telegram | Bot API | — | Anomaly push alerts |
| DDNS | Cloudflare DDNS service | custom | Keeps DNS A records current for dynamic IP |

**Hardware**: Single homelab machine — Intel i5-8th gen, 24 GB RAM, running Ubuntu. No cloud dependency except Cloudflare DNS and Let's Encrypt ACME.

---

## Package Management & Toolchain

| Tool | Role |
|------|------|
| `uv` | Python package manager + workspace |
| `hatchling` | Build backend for all packages |
| `ruff` | Linter (line-length 100, target py312) |
| `mypy` (strict) | Type checker |
| `pytest` | Test runner (`unit` + `integration` markers) |
| Python 3.12 | Runtime |

---

## Technology Decision Summary

| Decision | Chosen | Alternative considered | Reason |
|----------|--------|----------------------|--------|
| Message broker | Redpanda | Apache Kafka | No JVM; single binary; homelab-friendly |
| Table format | Apache Iceberg | Delta Lake / plain Parquet | ACID guarantees; Nessie catalog for branching |
| Batch engine | DuckDB | Spark | In-process; zero cluster overhead; fast on Parquet |
| Online DB | PostgreSQL | Redis | Low write frequency; no extra infra; asyncpg native |
| LLM | Ollama + qwen2.5:3b | OpenAI API | Free; offline; controllable; privacy |
| Vector DB | ChromaDB | pgvector | Simpler ops; dedicated ANN index; Python-native |
| Anomaly model | IsolationForest | LSTM / Autoencoder | Explainable; fast training; works with < 100 samples |
| Orchestration | Prefect | Airflow / Dagster | Modern Python-native API; local server easy to run |
