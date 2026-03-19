"""Defines the medallion batch pipeline stages and execution graph."""

import logging

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