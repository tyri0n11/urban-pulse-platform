FROM python:3.12-slim
WORKDIR /app
RUN pip install uv

# Copy workspace files needed for dependency resolution
COPY pyproject.toml uv.lock ./
COPY packages/ packages/
COPY apps/weather-ingestion/ apps/weather-ingestion/

# Sync only weather-ingestion package and its dependencies into .venv
RUN uv sync --package weather-ingestion --no-dev

CMD [".venv/bin/python", "-m", "weather_ingestion.main"]
