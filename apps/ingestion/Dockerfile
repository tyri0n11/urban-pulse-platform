FROM python:3.12-slim
WORKDIR /app
RUN pip install uv

# Copy workspace files needed for dependency resolution
COPY pyproject.toml uv.lock ./
COPY packages/ packages/
COPY apps/ingestion/ apps/ingestion/
COPY routes.json .

# Sync only ingestion package and its dependencies into .venv
RUN uv sync --package ingestion --no-dev

CMD [".venv/bin/python", "-m", "ingestion.main"]
