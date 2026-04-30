FROM python:3.12-slim AS builder
WORKDIR /app
RUN pip install uv

COPY pyproject.toml uv.lock ./
COPY packages/ packages/
COPY apps/ml/ apps/ml/

RUN uv sync --package ml --no-dev --no-editable

FROM python:3.12-slim
WORKDIR /app

COPY --from=builder /app/.venv .venv
COPY --from=builder /app/apps/ml /app/apps/ml

EXPOSE 8000
CMD [".venv/bin/uvicorn", "ml.api:app", "--host", "0.0.0.0", "--port", "8000"]
