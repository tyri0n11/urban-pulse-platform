FROM python:3.12-slim
WORKDIR /app
RUN pip install uv

COPY pyproject.toml uv.lock ./
COPY packages/ packages/
COPY apps/online/ apps/online/

RUN uv sync --package online --no-dev

ENV PYTHONPATH=/app/apps/online/src

CMD [".venv/bin/python", "-m", "online.app"]
