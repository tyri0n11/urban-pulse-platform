"""Defines the medallion batch pipeline stages and execution graph."""

import logging
from datetime import datetime, timezone

import httpx
from prefect import flow, task

from batch.jobs import baseline_learning, bronze_to_silver, silver_to_gold

logger = logging.getLogger(__name__)

_ML_SERVICE_URL = "http://ml-service:8000"


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(name="microbatch-bronze-to-silver", retries=3, retry_delay_seconds=60)  # type: ignore[untyped-decorator]
def task_microbatch_bronze_to_silver() -> int:
    return bronze_to_silver.microbatch()


@task(name="silver-to-gold", retries=3, retry_delay_seconds=60)  # type: ignore[untyped-decorator]
def task_silver_to_gold() -> int:
    return silver_to_gold.run()


@task(name="baseline-learning", retries=3, retry_delay_seconds=60)  # type: ignore[untyped-decorator]
def task_baseline_learning() -> int:
    return baseline_learning.run()


@task(name="trigger-ml-retrain", retries=2, retry_delay_seconds=30)  # type: ignore[untyped-decorator]
def task_trigger_ml_retrain() -> dict[str, object]:
    """Trigger ML training via HTTP call to ml-service."""
    logger.info("Triggering ML retrain at %s", _ML_SERVICE_URL)
    resp = httpx.post(f"{_ML_SERVICE_URL}/train", timeout=300)
    resp.raise_for_status()
    result = resp.json()
    logger.info("ML retrain result: status=%s, run_id=%s", result["status"], result["run_id"])
    return result  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Flows
# ---------------------------------------------------------------------------

@flow(name="microbatch", log_prints=True)  # type: ignore[untyped-decorator]
def microbatch() -> None:
    """Fast-path microbatch: promote new bronze records to silver.

    Runs every 5 min so the silver layer stays close to real-time
    while keeping each DuckDB scan small.
    """
    task_microbatch_bronze_to_silver()


@flow(name="hourly-gold", log_prints=True)  # type: ignore[untyped-decorator]
def hourly_gold() -> None:
    """Hourly aggregation: silver → gold.

    Lightweight incremental job — only aggregates the current hour's
    silver data into gold summaries. No full scans.
    """
    task_silver_to_gold()


@flow(name="retrain", log_prints=True)  # type: ignore[untyped-decorator]
def retrain() -> None:
    """Periodic baseline recompute + model retrain.

    Runs every 6 hours. Baseline does a full gold scan (expensive),
    then triggers ML service to retrain on fresh baselines.
    """
    baseline_rows = task_baseline_learning()
    task_trigger_ml_retrain(wait_for=[baseline_rows])


# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------

@task(name="bootstrap-traffic-silver", retries=3, retry_delay_seconds=60)  # type: ignore[untyped-decorator]
def bootstrap_traffic_silver() -> int:
    """Full backfill: all bronze → silver."""
    logger.info("Bootstrapping traffic silver with an immediate run")
    return bronze_to_silver.bootstrap()


@task(name="bootstrap-silver-to-gold", retries=3, retry_delay_seconds=60)  # type: ignore[untyped-decorator]
def bootstrap_silver_to_gold() -> int:
    """Full re-aggregate: all silver → gold hourly."""
    logger.info("Bootstrapping silver to gold")
    return silver_to_gold.bootstrap()


@task(name="bootstrap-baseline-learning", retries=3, retry_delay_seconds=60)  # type: ignore[untyped-decorator]
def bootstrap_baseline_learning() -> int:
    """Recompute baselines from all gold data."""
    logger.info("Bootstrapping baseline learning")
    return baseline_learning.run()


@flow(name="bootstrap", log_prints=True)  # type: ignore[untyped-decorator]
def bootstrap() -> None:
    """Full medallion bootstrap: bronze → silver → gold → baseline.

    Runs each stage sequentially with explicit dependencies.
    """
    logger.info("Bootstrapping full medallion pipeline")
    silver_rows = bootstrap_traffic_silver()
    gold_rows = bootstrap_silver_to_gold(wait_for=[silver_rows])
    bootstrap_baseline_learning(wait_for=[gold_rows])


# ---------------------------------------------------------------------------
# Backfill
# ---------------------------------------------------------------------------

@task(name="backfill-traffic-silver", retries=2, retry_delay_seconds=30)  # type: ignore[untyped-decorator]
def task_backfill_traffic_silver(from_dt: datetime, to_dt: datetime) -> int:
    return bronze_to_silver.backfill(from_dt=from_dt, to_dt=to_dt)


@flow(name="backfill", log_prints=True)  # type: ignore[untyped-decorator]
def backfill(
    from_dt: datetime,
    to_dt: datetime | None = None,
) -> None:
    """Manually backfill silver for a missed time range.

    Trigger from Prefect UI or CLI:
        prefect deployment run backfill/backfill-deployment \\
            -p from_dt="2026-03-21T00:00:00+00:00" \\
            -p to_dt="2026-03-21T23:00:00+00:00"
    """
    resolved_to = to_dt or datetime.now(timezone.utc)
    logger.info("Backfilling silver from %s to %s", from_dt, resolved_to)
    task_backfill_traffic_silver(from_dt=from_dt, to_dt=resolved_to)
