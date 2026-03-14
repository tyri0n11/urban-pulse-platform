.PHONY: dev down logs status lint typecheck test test-unit test-integration build build-ingestion

dev:
	docker compose -f infra/docker/docker-compose.base.yaml -f infra/docker/docker-compose.dev.yaml up -d

down:
	docker compose -f infra/docker/docker-compose.base.yaml -f infra/docker/docker-compose.dev.yaml down

logs:
	docker compose -f infra/docker/docker-compose.base.yaml -f infra/docker/docker-compose.dev.yaml logs -f

status:
	docker compose -f infra/docker/docker-compose.base.yaml -f infra/docker/docker-compose.dev.yaml ps

logs-ingestion:
	docker compose -f infra/docker/docker-compose.base.yaml -f infra/docker/docker-compose.dev.yaml logs -f ingestion

shell-serving:
	docker compose -f infra/docker/docker-compose.base.yaml exec serving /bin/bash

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
	docker compose -f infra/docker/docker-compose.base.yaml build

build-ingestion:
	docker compose -f infra/docker/docker-compose.base.yaml -f infra/docker/docker-compose.dev.yaml build ingestion

build-serving:
	docker compose -f infra/docker/docker-compose.base.yaml build serving
