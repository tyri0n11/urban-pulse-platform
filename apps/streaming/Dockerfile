FROM python:3.12-slim AS builder
WORKDIR /app
RUN pip install uv

COPY pyproject.toml uv.lock ./
COPY packages/ packages/
COPY apps/streaming/ apps/streaming/

RUN uv sync --package streaming --no-dev --no-editable

FROM python:3.12-slim
WORKDIR /app

COPY --from=builder /app/.venv .venv
COPY --from=builder /app/apps/streaming /app/apps/streaming

ENV PYTHONPATH=/app/apps/streaming/src/streaming

CMD [".venv/bin/python", "/app/apps/streaming/src/streaming/main.py"]
