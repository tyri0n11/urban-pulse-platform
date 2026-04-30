FROM python:3.12-slim AS builder
WORKDIR /app
RUN pip install uv

COPY pyproject.toml uv.lock ./
COPY packages/ packages/
COPY apps/traffic-ingestion/ apps/traffic-ingestion/
COPY routes.json routes.json

RUN uv sync --package traffic-ingestion --no-dev --no-editable

FROM python:3.12-slim
WORKDIR /app

COPY --from=builder /app/.venv .venv
COPY --from=builder /app/apps/traffic-ingestion /app/apps/traffic-ingestion
COPY --from=builder /app/routes.json routes.json

CMD [".venv/bin/python", "-m", "traffic_ingestion.main"]
