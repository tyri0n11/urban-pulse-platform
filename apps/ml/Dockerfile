FROM python:3.12-slim
WORKDIR /app
RUN pip install uv

# Copy workspace files needed for dependency resolution
COPY pyproject.toml uv.lock ./
COPY packages/ packages/
COPY apps/ml/ apps/ml/

# Sync only ml package and its dependencies into .venv
RUN uv sync --package ml --no-dev

EXPOSE 8000
CMD [".venv/bin/uvicorn", "ml.api:app", "--host", "0.0.0.0", "--port", "8000"]
