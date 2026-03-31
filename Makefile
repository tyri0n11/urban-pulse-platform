.PHONY: dev down logs status lint typecheck test test-unit test-integration build build-ingestion bootstrap train pull-model \
        prod prod-down prod-logs prod-status prod-build prod-bootstrap prod-train prod-setup

COMPOSE     = docker compose --env-file .env -f infra/docker/docker-compose.base.yaml -f infra/docker/docker-compose.dev.yaml
COMPOSE_PROD = docker compose --env-file .env.prod -f infra/docker/docker-compose.prod.yaml

dev:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f

status:
	$(COMPOSE) ps

logs-ingestion:
	$(COMPOSE) logs -f ingestion

shell-serving:
	$(COMPOSE) exec serving /bin/bash

lint:
	uv run ruff check .

typecheck:
	uv run mypy apps/ packages/

test:
	uv run pytest --cov

test-unit:
	uv run pytest -m unit

test-integration:
	uv run pytest -m integration

build:
	$(COMPOSE) build

build-ingestion:
	$(COMPOSE) build ingestion

build-serving:
	$(COMPOSE) build serving

bootstrap:
	$(COMPOSE) exec batch .venv/bin/python -m batch.bootstrap_cli

pull-model:
	@echo "Pulling $(OLLAMA_MODEL) into Ollama (this may take a few minutes)..."
	$(COMPOSE) exec ollama ollama pull $(or $(OLLAMA_MODEL),qwen2.5:3b)
	@echo "Model ready."

train:
	@echo "Triggering training via ML service API..."
	@curl -sf http://localhost:8000/health > /dev/null 2>&1 || { echo "ML service not running — start with 'make dev'"; exit 1; }
	@curl -s -X POST http://localhost:8000/train | python -m json.tool

make setup:
	cat .env.example > infra/docker/.env
	nano infra/docker/.env

# ---------------------------------------------------------------------------
# Production targets (homelab)
# ---------------------------------------------------------------------------

prod-setup:
	@echo "Setting up production data directories..."
	sudo mkdir -p /opt/urban-pulse/redpanda \
	              /opt/urban-pulse/minio \
	              /opt/urban-pulse/nessie \
	              /opt/urban-pulse/dremio \
	              /opt/urban-pulse/mlflow \
	              /opt/urban-pulse/prefect \
	              /opt/urban-pulse/postgres \
	              /opt/urban-pulse/loki \
	              /opt/urban-pulse/grafana \
	              /opt/urban-pulse/traefik
	sudo chown -R $(USER) /opt/urban-pulse
	touch /opt/urban-pulse/traefik/acme.json
	chmod 600 /opt/urban-pulse/traefik/acme.json
	@echo "Copying env template..."
	cp -n .env.prod.example .env.prod
	@echo "Edit .env.prod with your real values, then run: make prod"

prod:
	$(COMPOSE_PROD) up -d

prod-down:
	$(COMPOSE_PROD) down

prod-build:
	$(COMPOSE_PROD) build

prod-update-ui:
	cd v0-urban-pulse-dashboard && git pull origin main
	$(COMPOSE_PROD) build --no-cache ui
	$(COMPOSE_PROD) up -d --force-recreate ui

prod-logs:
	$(COMPOSE_PROD) logs -f

prod-status:
	$(COMPOSE_PROD) ps

prod-bootstrap:
	$(COMPOSE_PROD) exec batch .venv/bin/python -m batch.bootstrap_cli

prod-train:
	@echo "Triggering training via ML service API..."
	@curl -sf http://localhost:8000/health > /dev/null 2>&1 || { echo "ML service not running — start with 'make prod'"; exit 1; }
	@curl -s -X POST http://localhost:8000/train | python -m json.tool
