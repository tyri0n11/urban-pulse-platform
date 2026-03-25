.PHONY: dev down logs status lint typecheck test test-unit test-integration build build-ingestion bootstrap train

COMPOSE = docker compose --env-file .env -f infra/docker/docker-compose.base.yaml -f infra/docker/docker-compose.dev.yaml

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
	$(COMPOSE) build batch
	$(COMPOSE) up -d minio nessie
	@echo "Waiting for Nessie to be healthy..."
	@until docker inspect --format='{{.State.Health.Status}}' nessie 2>/dev/null | grep -q healthy; do sleep 2; done
	$(COMPOSE) run --rm batch .venv/bin/python -m batch.bootstrap_cli
	@echo "Bootstrap complete."

train:
	@echo "Triggering training via ML service API..."
	@curl -sf http://localhost:8000/health > /dev/null 2>&1 || { echo "ML service not running — start with 'make dev'"; exit 1; }
	@curl -s -X POST http://localhost:8000/train | python -m json.tool

make setup:
	cat .env.example > infra/docker/.env
	nano infra/docker/.env
