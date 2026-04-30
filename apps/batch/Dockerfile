FROM python:3.12-slim AS builder
WORKDIR /app
RUN pip install uv

COPY pyproject.toml uv.lock ./
COPY packages/ packages/
COPY apps/batch/ apps/batch/

RUN uv sync --package batch --no-dev --no-editable

# Pre-install DuckDB extensions during build (requires internet at build time,
# but not at runtime — avoids download failures inside the running container).
RUN .venv/bin/python -c "import duckdb; duckdb.connect().execute('INSTALL httpfs')"

FROM python:3.12-slim
WORKDIR /app

COPY --from=builder /app/.venv .venv
COPY --from=builder /app/apps/batch /app/apps/batch
COPY --from=builder /root/.duckdb /root/.duckdb

CMD [".venv/bin/python", "-m", "batch.main"]
