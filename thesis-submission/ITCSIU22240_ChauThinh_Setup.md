# Local Development Guide

A step-by-step guide to get **Urban Pulse Platform** running on your machine from a clean checkout. Aimed at people who have never touched this repo before.

> If you just want the condensed cheat-sheet, see the **Quick Start (Dev)** section in [README.md](../README.md). This document is the slower, explained version with verification steps and troubleshooting.

---

## 1. What you're about to run

Urban Pulse is a real-time traffic anomaly platform for Ho Chi Minh City. Locally, `make dev` brings up a stack of ~14 containers (Kafka, MinIO, Postgres, MLflow, Prefect, ChromaDB, plus the project's own ingestion/streaming/ml/serving apps). The LLM (Ollama) runs **natively** on your machine, not in Docker.

You don't need to understand all of it to run it — just follow the steps. The mental model:

```
VietMap API → ingestion → Kafka → streaming → MinIO (data lake)
                               └→ online → Postgres (live features)
batch (Prefect) → builds Iceberg tables + trains the model + indexes RAG
serving (FastAPI) → REST + SSE + LLM, read by the Next.js dashboard
```

---

## 2. Prerequisites

Install these first. Versions below are what the project targets.

| Tool | Why | Install (macOS) |
|------|-----|-----------------|
| **Docker Desktop** | Runs the whole stack | [docker.com](https://www.docker.com/products/docker-desktop/) |
| **uv** | Python package manager (do **not** use `pip`/`python` directly) | `brew install uv` |
| **Ollama** | Local LLM inference (Metal GPU on Mac) | `brew install ollama` |
| **make** | Runs the shortcut commands | Preinstalled on macOS / `xcode-select --install` |
| **git** | Clone the repos | Preinstalled |

> **Linux:** use Docker Engine + Compose plugin instead of Docker Desktop. Ollama can run in a container or natively — if native, the compose files expect it at `host.docker.internal:11434`.

**Resource sizing:** Docker Desktop → Settings → Resources. Give Docker at least **8 GB RAM** (the reference homelab runs 24 GB). The model training and Ollama embeddings are the heaviest parts.

You'll also need a **VietMap API key** (the traffic data source). Without it, ingestion has no live data — but the rest of the stack still boots, and `make bootstrap` can run on any data already in MinIO.

---

## 3. Clone the repos

The UI lives in a **sibling repo** that must sit next to the platform repo, because the dev compose file references it by relative path.

```bash
git clone git@github.com:tyri0n11/urban-pulse-platform.git
cd urban-pulse-platform
git clone git@github.com:tyri0n11/v0-urban-pulse-dashboard.git
```

After this you should have:

```
urban-pulse-platform/
  v0-urban-pulse-dashboard/   ← UI, cloned inside
```

> If the UI fails to build later, double-check it's at this exact path — the compose build context is `../../v0-urban-pulse-dashboard`.

---

## 4. Create your `.env`

```bash
cp .env.example .env
```

Open `.env` and fill in real values:

```env
VIETMAP_API_KEY=<your key>
MINIO_ROOT_USER=<username>
MINIO_ROOT_PASSWORD=<password — no @ symbol>
NESSIE_S3_ACCESS_KEY=<same as MINIO_ROOT_USER>
NESSIE_S3_SECRET_KEY=<same as MINIO_ROOT_PASSWORD>
POSTGRES_USER=urbanpulse
POSTGRES_PASSWORD=<password — no @ symbol>
```

> ⚠️ **Do not put an `@` in any password.** asyncpg parses the Postgres connection string and `@` is the host separator — it will break the connection in a confusing way. Stick to letters and digits to be safe.
>
> ℹ️ `NESSIE_S3_*` must match `MINIO_ROOT_*` exactly — Nessie uses MinIO as its S3 backend.

---

## 5. Pull the Ollama models

Ollama runs natively, so pull the models **on your host** (not in a container):

```bash
ollama pull qwen2.5:3b        # the chat / explanation LLM
ollama pull nomic-embed-text  # 768-dim embeddings for RAG
```

This downloads a few GB and can take several minutes on the first run. Verify:

```bash
ollama list   # both models should appear
```

> There is also a `make pull-model` helper, but pulling manually the first time makes failures easier to see.

---

## 6. Start the stack

```bash
make dev
```

This builds (first time only) and starts all containers in the background. The first build is slow — grab a coffee. Subsequent `make dev` runs are fast.

Watch it come up:

```bash
make status        # docker ps — all services should reach "healthy"/"running"
make logs          # tail everything (Ctrl-C to stop tailing; containers keep running)
make logs-serving  # just the serving API
```

Give it **~2 minutes** for health checks to pass before moving on.

---

## 7. First-time bootstrap

A fresh stack has empty data and no trained model. Run these **once** after the first `make dev`:

```bash
make bootstrap   # creates Iceberg tables and runs the medallion pipeline from scratch
make train       # trains the IsolationForest model and pushes it to MLflow
```

Then build the RAG index so the LLM endpoints have context:

```bash
# Fast: weather + anomaly events only (~60s)
docker exec batch-service .venv/bin/prefect deployment run \
  rag-index/rag-index-deployment --param index_patterns=false

# Full: also indexes traffic patterns (~5–10 min, Ollama embedding is the bottleneck)
docker exec batch-service .venv/bin/prefect deployment run \
  rag-index/rag-index-deployment
```

After bootstrap, the Prefect flows run on their own schedules (microbatch every 5 min, gold every 1 hr, retrain every 6 hrs) — you don't need to trigger them manually for normal dev.

---

## 8. Verify it works

Open these in a browser:

| URL | What you should see |
|-----|---------------------|
| http://localhost:3000 | **Dashboard** — map + live routes |
| http://localhost:8001/docs | Serving API (Swagger) |
| http://localhost:4200 | Prefect — flows scheduled/running |
| http://localhost:5000 | MLflow — a registered `traffic-anomaly-iforest` model |
| http://localhost:9001 | MinIO Console — `urban-pulse` bucket with parquet files |
| http://localhost:3001 | Grafana — metrics + logs |
| http://localhost:8010 | ChromaDB (RAG vector store) |

Quick API smoke test from the terminal:

```bash
curl -s http://localhost:8001/health/ready
curl -s http://localhost:8001/online/features | head
curl -s http://localhost:8000/health        # ML service
```

If the dashboard shows routes and the API returns data, you're up. 🎉

---

## 9. Everyday commands

```bash
# Stack lifecycle
make dev            # start full stack
make down           # stop all containers
make status         # what's running
make logs           # tail all logs

# Rebuild after code changes
make build-serving  # rebuild just the serving image
make build          # rebuild everything

# Code quality (run these before committing)
make lint           # ruff
make typecheck      # mypy --strict
make test           # pytest (unit + integration)
make test-unit
make test-integration
```

All Python commands must go through **`uv run`** — e.g. `uv run pytest apps/ingestion/tests/test_orchestrator.py`. Never call `pip` or bare `python`.

**Rebuild and restart a single service after editing its code:**

```bash
docker compose --env-file .env -f infra/docker/docker-compose.dev.yaml build --no-cache serving
docker compose --env-file .env -f infra/docker/docker-compose.dev.yaml up -d serving
```

**Trigger a Prefect flow by hand:**

```bash
docker exec batch-service .venv/bin/prefect deployment run microbatch/microbatch-deployment
docker exec batch-service .venv/bin/prefect deployment run retrain/retrain-deployment
```

---

## 10. Troubleshooting

| Symptom | Likely cause / fix |
|---------|--------------------|
| Postgres connection errors at startup | An `@` in `POSTGRES_PASSWORD`. Remove it, then `make down && make dev`. |
| Serving `/chat`, `/rca`, `/explain` hang or error | Ollama not running or models not pulled. Run `ollama list`; re-run `ollama pull qwen2.5:3b nomic-embed-text`. |
| UI container fails to build | `v0-urban-pulse-dashboard` not cloned **inside** `urban-pulse-platform/`. |
| Dashboard loads but shows no routes | Bootstrap/data not run yet. Run `make bootstrap`, then `make train`. |
| `/predict` returns no model | Model not trained. Run `make train`; check MLflow at :5000. |
| LLM answers with empty/weak context | RAG index not built. Run the `rag-index` commands in §7. |
| A service is `unhealthy` in `make status` | `make logs-<service>` (or `make logs`) to read the error; often it's a missing/typo'd `.env` value. |
| Port already in use | Another process owns the port (e.g. Grafana moved to **3001** to avoid the Next.js **3000** clash). Stop the conflicting app or change the mapping in the dev compose file. |
| Changes to Python code not taking effect | Rebuild that service's image (§9) — containers run baked images, not your live source. |

To start completely fresh (⚠️ destroys local data/volumes):

```bash
make down
docker compose --env-file .env -f infra/docker/docker-compose.base.yaml -f infra/docker/docker-compose.dev.yaml down -v
make dev && make bootstrap && make train
```

---

## 11. Where to go next

- [README.md](../README.md) — architecture, API endpoint map, design decisions
- [CLAUDE.md](../CLAUDE.md) — detailed architecture & data-flow reference
- `apps/*/` — each service is a `uv` workspace member with its own `pyproject.toml`
- Prefect UI (:4200) — watch the medallion pipeline and retrain flows run live
