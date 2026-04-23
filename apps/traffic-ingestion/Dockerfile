FROM python:3.12-slim
WORKDIR /app
RUN pip install uv

# Copy workspace files needed for dependency resolution
COPY pyproject.toml uv.lock ./
COPY packages/ packages/
COPY apps/traffic-ingestion/ apps/traffic-ingestion/
COPY routes.json .

# Sync only traffic-ingestion package and its dependencies into .venv
RUN uv sync --package traffic-ingestion --no-dev

CMD [".venv/bin/python", "-m", "traffic_ingestion.main"]
