FROM python:3.12-slim
WORKDIR /app
RUN pip install uv

# Copy workspace files needed for dependency resolution
COPY pyproject.toml uv.lock ./
COPY packages/ packages/
COPY apps/streaming/ apps/streaming/

# Sync only streaming package and its dependencies into .venv
RUN uv sync --package streaming --no-dev

# Bare imports in streaming (e.g. `from consumers.kafka import ...`) resolve
# relative to the package source directory
ENV PYTHONPATH=/app/apps/streaming/src/streaming

CMD [".venv/bin/python", "/app/apps/streaming/src/streaming/main.py"]
