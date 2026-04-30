FROM python:3.12-slim AS builder
WORKDIR /app
RUN pip install uv

COPY pyproject.toml uv.lock ./
COPY packages/ packages/
COPY apps/serving/ apps/serving/
COPY routes.json routes.json

RUN uv sync --package serving --no-dev --no-editable

FROM python:3.12-slim
WORKDIR /app

COPY --from=builder /app/.venv .venv
COPY --from=builder /app/apps/serving /app/apps/serving
COPY --from=builder /app/routes.json /app/routes.json

CMD [".venv/bin/python", "-m", "serving.main"]


