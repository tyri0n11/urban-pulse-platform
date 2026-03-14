FROM python:3.12-slim
WORKDIR /app
RUN pip install uv

# Copy and install workspace packages
COPY packages/core packages/core
COPY packages/infra-clients packages/infra-clients
RUN uv pip install --system packages/core packages/infra-clients

# Copy and install ingestion app
COPY apps/ingestion apps/ingestion
RUN uv pip install --system apps/ingestion

# Routes config must be present at runtime
COPY routes.json .

CMD ["python", "-m", "ingestion.main"]
