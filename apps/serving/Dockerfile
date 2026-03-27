FROM python:3.12-slim
WORKDIR /app
RUN pip install uv

COPY pyproject.toml uv.lock ./
COPY packages/ packages/
COPY apps/serving/ apps/serving/
COPY routes.json routes.json

RUN uv sync --package serving --no-dev

CMD [".venv/bin/python", "-m", "serving.main"]
