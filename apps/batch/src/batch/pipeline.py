"""Defines the medallion batch pipeline stages and execution graph."""

import logging
from datetime import datetime, timezone

from prefect import flow, task

from batch.jobs import bronze_to_silver

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(name="microbatch-bronze-to-silver", retries=3, retry_delay_seconds=60)  # type: ignore[untyped-decorator]
def task_microbatch_bronze_to_silver() -> int:
    return bronze_to_silver.microbatch()


# ---------------------------------------------------------------------------
# Flows
# ---------------------------------------------------------------------------

@flow(name="microbatch", log_prints=True)  # type: ignore[untyped-decorator]
def microbatch() -> None:
    """Fast-path microbatch: promote new bronze records to silver.

    Runs on a tight schedule (every 5 min) so the silver layer stays
    close to real-time while keeping each DuckDB scan small.
    """
    task_microbatch_bronze_to_silver()


# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------

@task(name="bootstrap-traffic-silver", retries=3, retry_delay_seconds=60)  # type: ignore[untyped-decorator]
def bootstrap_traffic_silver() -> int:
    """Run the bronze_to_silver job once, without waiting for the scheduled run."""
    logger.info("Bootstrapping traffic silver with an immediate run")
    return bronze_to_silver.bootstrap()


@flow(name="bootstrap", log_prints=True)  # type: ignore[untyped-decorator]
def bootstrap() -> None:
    """Run the pipeline once immediately, without waiting for the scheduled run."""
    logger.info("Bootstrapping medallion pipeline with an immediate run")
    bootstrap_traffic_silver()


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