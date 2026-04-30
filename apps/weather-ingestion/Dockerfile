FROM python:3.12-slim AS builder
WORKDIR /app
RUN pip install uv

COPY pyproject.toml uv.lock ./
COPY packages/ packages/
COPY apps/weather-ingestion/ apps/weather-ingestion/

RUN uv sync --package weather-ingestion --no-dev --no-editable

FROM python:3.12-slim
WORKDIR /app

COPY --from=builder /app/.venv .venv
COPY --from=builder /app/apps/weather-ingestion /app/apps/weather-ingestion

CMD [".venv/bin/python", "-m", "weather_ingestion.main"]
