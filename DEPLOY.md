# Urban Pulse — Production Deployment

**Target**: Homelab (192.168.1.9), domain `tyr1on.io.vn`, Docker Compose + Traefik

---

## Prerequisites (one-time, already done)

- [x] Cloudflare A records: `@` and `*` → `171.248.102.63` (DNS-only, no proxy)
- [x] Router port forwarding: 80 + 443 → 192.168.1.9
- [x] `infra/traefik/traefik.yml` — HTTP challenge, email set
- [x] `.gitignore` includes `.env.prod`, `infra/traefik/dynamic.yml`, `**/acme.json`

---

## First-Time Server Setup

### 1. Clone repos

```bash
cd ~
git clone git@github.com:tyri0n11/urban-pulse-platform.git
cd urban-pulse-platform
git clone git@github.com:tyri0n11/v0-urban-pulse-dashboard.git
```

> UI repo must be inside `urban-pulse-platform/` — docker-compose context is `../../v0-urban-pulse-dashboard` relative to `infra/docker/`.

### 2. Create directories and acme.json

```bash
make prod-setup
```

This creates:
```
/opt/urban-pulse/{redpanda,minio,nessie,dremio,mlflow,prefect,postgres,loki,grafana,traefik}/
/opt/urban-pulse/traefik/acme.json  (chmod 600)
```

Fix ownership for services that need specific UIDs:
```bash
sudo chown -R 999:999 /opt/urban-pulse/dremio
sudo chown -R 10001:10001 /opt/urban-pulse/loki
```

> **ChromaDB** uses a named Docker volume (`chroma-data`) — no manual directory needed.

### 3. Create `.env.prod`

```bash
cp .env.prod.example .env.prod
chmod 600 .env.prod
nano .env.prod
```

Fill in:
```env
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=<strong password, NO @ symbol>
NESSIE_S3_ACCESS_KEY=minioadmin          # same as MINIO_ROOT_USER
NESSIE_S3_SECRET_KEY=<same as MINIO_ROOT_PASSWORD>
POSTGRES_USER=urbanpulse
POSTGRES_PASSWORD=<strong password, NO @ symbol>
VIETMAP_API_KEY=<your key>
NEXT_PUBLIC_API_URL=https://api.tyr1on.io.vn
NEXT_PUBLIC_TRAFFIC_SSE_URL=https://api.tyr1on.io.vn/events/traffic
```

> **CRITICAL**: No `@` in passwords — asyncpg parses DSN and `@` breaks the host separator.

### 4. Create Traefik basicauth file

Generate htpasswd hash (do NOT commit this file):
```bash
htpasswd -nb admin YourPassword
# or: docker run --rm httpd:alpine htpasswd -nb admin YourPassword
```

Create `infra/traefik/dynamic.yml` (gitignored):
```yaml
http:
  middlewares:
    basicauth:
      basicAuth:
        users:
          - "admin:$apr1$xxxx$xxxx"   # paste hash here, escape $ as $$
```

> The `$` in htpasswd hashes must be written as `$$` in YAML strings inside `users:` list.

---

## Deploy

### Build images

```bash
make prod-build
```

This builds: `traffic-ingestion`, `weather-ingestion`, `streaming`, `online`, `batch`, `ml`, `serving`, `ui`.

### Start stack

```bash
make prod
```

Wait ~2 min for all services to become healthy. Check status:
```bash
make prod-status
make prod-logs
```

### Bootstrap medallion pipeline (first time only)

```bash
make prod-bootstrap
```

Runs: MinIO bucket creation → Nessie namespace → Iceberg schema → backfill if needed.

### Trigger first ML training

```bash
make prod-train
```

### Pull Ollama models (first time only)

```bash
docker exec ollama ollama pull qwen2.5:3b
docker exec ollama ollama pull nomic-embed-text
```

### Initialize RAG index (first time only)

```bash
# Fast path — chỉ index weather (Open-Meteo 7 ngày) + anomaly events (~60s)
docker exec batch-service .venv/bin/prefect deployment run rag-index/rag-index-deployment \
  --param index_patterns=false
```

> **Không dùng `index_patterns=true` lần đầu** — phải embed ~3360 traffic pattern docs qua Ollama, mất 5–10 phút và có thể timeout. Traffic patterns sẽ tự được index ở lần `retrain` flow đầu tiên (chạy sau 6h).

Subsequent runs happen automatically:
- `hourly-gold` (every 1h) → re-index anomaly events + weather
- `retrain` (every 6h) → re-index traffic patterns (full gold scan)

---

## Ongoing Operations

| Command | What it does |
|---------|-------------|
| `make prod` | Start all services |
| `make prod-down` | Stop all services |
| `make prod-build` | Rebuild all images |
| `make prod-logs` | Tail logs (all services) |
| `make prod-status` | `docker ps` for prod stack |
| `make prod-train` | POST to ml-service to retrain IsolationForest |

### Rebuild a single service after code change

```bash
# Serving
docker compose --env-file .env.prod -f infra/docker/docker-compose.prod.yaml build --no-cache serving
docker compose --env-file .env.prod -f infra/docker/docker-compose.prod.yaml up -d serving

# Traffic ingestion
docker compose --env-file .env.prod -f infra/docker/docker-compose.prod.yaml build --no-cache traffic-ingestion
docker compose --env-file .env.prod -f infra/docker/docker-compose.prod.yaml up -d traffic-ingestion
```

> Service names in docker-compose: `traffic-ingestion`, `weather-ingestion`, `streaming`, `online`, `batch`, `ml`, `serving`, `ui`.

### Pull latest code and redeploy

```bash
git pull
cd v0-urban-pulse-dashboard && git pull && cd ..
make prod-build
make prod
```

---

## Service URLs (production)

| URL | Service | Auth |
|-----|---------|------|
| `https://tyr1on.io.vn` | UI Dashboard | — |
| `https://api.tyr1on.io.vn` | Serving API + SSE | — |
| `https://grafana.tyr1on.io.vn` | Grafana | Grafana login |
| `https://mlflow.tyr1on.io.vn` | MLflow | basicauth |
| `https://prefect.tyr1on.io.vn` | Prefect UI | basicauth |
| `https://minio.tyr1on.io.vn` | MinIO Console | MinIO login |
| `https://redpanda.tyr1on.io.vn` | Redpanda Console | basicauth |
| `https://dremio.tyr1on.io.vn` | Dremio | Dremio login |
| `http://192.168.1.9:8080` | Traefik dashboard | LAN only |

---

## Troubleshooting

### Serving can't connect to Postgres

Check DSN has no `@` in password. Reset Postgres if needed:
```bash
docker compose --env-file .env.prod -f infra/docker/docker-compose.prod.yaml stop postgres
sudo rm -rf /opt/urban-pulse/postgres
docker compose --env-file .env.prod -f infra/docker/docker-compose.prod.yaml up -d postgres
```

### Let's Encrypt cert not issuing

- Verify Cloudflare DNS is DNS-only (no orange cloud)
- Verify port 80 forwarding is active on router
- Check `docker logs traefik` for ACME errors
- `acme.json` must be `chmod 600` and owned by root

### Loki or Nessie fails healthcheck

```bash
# Loki
sudo chown -R 10001:10001 /opt/urban-pulse/loki

# Nessie — JVM takes 60s to start, wait longer
docker compose --env-file .env.prod -f infra/docker/docker-compose.prod.yaml up -d nessie
```

### Dremio permission error

```bash
sudo chown -R 999:999 /opt/urban-pulse/dremio
```

### SSE not streaming data to UI

The old UI (`v0-urban-pulse-dashboard`) requires SSE payload to have `type: "traffic_update"` and `lag` key. If serving was rebuilt with a different ws.py version, check `apps/serving/src/serving/routers/ws.py` returns the correct format.

### No anomalies (z-score = null)

`stddev_duration_minutes = 0` when all observations have identical duration. Needs natural variation over time (VietMap API returning different values). `iforest_anomaly` may still trigger independently.

### Telegram webhook (optional)

The Telegram bot (`@tyr1on_system_alert_bot`) receives anomaly alerts automatically via the `alert` Prefect flow. To also enable chat:

```bash
# Register webhook pointing to the serving API
curl "https://api.tyr1on.io.vn/telegram/set-webhook?url=https://api.tyr1on.io.vn"
```

Verify:
```bash
curl "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/getWebhookInfo"
```

Bot credentials are in `.env.prod` as `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` (gitignored).

---

### Git — never commit these files

```
.env.prod
infra/traefik/dynamic.yml
**/acme.json
```

---

## Security Notes

- `.env.prod` and `dynamic.yml` were accidentally committed once — required `git filter-repo` history rewrite and key rotation. Never use `git add .` or `git add -A`.
- All admin-facing services (MLflow, Prefect, Redpanda Console) are behind `basicauth@file` middleware via Traefik.
- Serving API and UI are public (no auth).
