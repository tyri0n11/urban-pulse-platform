"""FastAPI service for triggering ML training runs."""

import logging
import threading
from datetime import datetime, timezone

from fastapi import FastAPI
from pydantic import BaseModel

from ml.train import run_training

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Urban Pulse ML Service", version="0.1.0")


class TrainResponse(BaseModel):
    status: str
    run_id: str
    sample_count: int
    metrics: dict[str, float]


class TrainStatus(BaseModel):
    training: bool
    last_run_id: str
    last_status: str
    last_started_at: str


# Simple in-memory state for tracking current training
_state: dict[str, object] = {
    "training": False,
    "last_run_id": "",
    "last_status": "idle",
    "last_started_at": "",
}
_lock = threading.Lock()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/status", response_model=TrainStatus)
def status() -> dict[str, object]:
    return dict(_state)


@app.post("/train", response_model=TrainResponse)
def train() -> TrainResponse:
    """Trigger a training run synchronously."""
    with _lock:
        if _state["training"]:
            return TrainResponse(
                status="already_running", run_id="", sample_count=0, metrics={}
            )
        _state["training"] = True
        _state["last_started_at"] = datetime.now(timezone.utc).isoformat()

    try:
        result = run_training()
        _state["last_run_id"] = result.run_id
        _state["last_status"] = result.status
        return TrainResponse(
            status=result.status,
            run_id=result.run_id,
            sample_count=result.sample_count,
            metrics=result.metrics,
        )
    except Exception:
        _state["last_status"] = "failed"
        logger.exception("Training failed")
        raise
    finally:
        _state["training"] = False
