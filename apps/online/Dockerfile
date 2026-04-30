FROM python:3.12-slim AS builder
WORKDIR /app
RUN pip install uv

COPY pyproject.toml uv.lock ./
COPY packages/ packages/
COPY apps/online/ apps/online/

RUN uv sync --package online --no-dev --no-editable

FROM python:3.12-slim
WORKDIR /app

COPY --from=builder /app/.venv .venv
COPY --from=builder /app/apps/online /app/apps/online

ENV PYTHONPATH=/app/apps/online/src

CMD [".venv/bin/python", "-m", "online.app"]
