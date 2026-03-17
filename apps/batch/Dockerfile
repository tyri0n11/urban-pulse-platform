FROM python:3.12-slim
WORKDIR /app
RUN pip install uv

# Copy workspace files needed for dependency resolution
COPY pyproject.toml uv.lock ./
COPY packages/ packages/
COPY apps/batch/ apps/batch/

# Sync only batch package and its dependencies into .venv
RUN uv sync --package batch --no-dev

# Pre-install DuckDB extensions during build (requires internet at build time,
# but not at runtime — avoids download failures inside the running container).
RUN .venv/bin/python -c "import duckdb; duckdb.connect().execute('INSTALL httpfs')"

CMD [".venv/bin/python", "-m", "batch.main"]
