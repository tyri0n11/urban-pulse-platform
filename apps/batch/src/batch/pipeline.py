"""Defines the medallion batch pipeline stages and execution graph."""

import logging

from prefect import flow, task

from batch.jobs import baseline_learning, bronze_to_silver, silver_to_gold

logger = logging.getLogger(__name__)


@task(name="bronze-to-silver", retries=3, retry_delay_seconds=60)  # type: ignore[untyped-decorator]
def task_bronze_to_silver() -> int:
    return bronze_to_silver.run()


@task(name="silver-to-gold", retries=3, retry_delay_seconds=60)  # type: ignore[untyped-decorator]
def task_silver_to_gold() -> int:
    return silver_to_gold.run()


@task(name="baseline-learning", retries=2, retry_delay_seconds=300)  # type: ignore[untyped-decorator]
def task_baseline_learning() -> int:
    return baseline_learning.run()


@flow(name="data-pipeline", log_prints=True)  # type: ignore[untyped-decorator]
def traffic_pipeline() -> None:
    """Run the full bronze → silver → gold → baseline promotion pipeline."""
    b2s = task_bronze_to_silver.submit()
    # s2g = task_silver_to_gold.submit(wait_for=[b2s])
    # task_baseline_learning.submit(wait_for=[s2g])


'''
BOOTSTRAP TASKS
'''

@task(name="bootstrap-traffic-silver", retries=3, retry_delay_seconds=60)  # type: ignore[untyped-decorator]
def bootstrap_traffic_silver() -> int:
    """Run the bronze_to_silver job once, without waiting for the scheduled run."""
    logger.info("Bootstrapping traffic silver with an immediate run")
    return bronze_to_silver.bootstrap()

@flow(name="bootstrap", log_prints=True)  # type: ignore[untyped-decorator]
def bootstrap() -> None:
    """Backfill silver, then run gold and baseline on the promoted data."""
    logger.info("Bootstrapping medallion pipeline with an immediate run")
    b2s = bootstrap_traffic_silver.submit()
    s2g = task_silver_to_gold.submit(wait_for=[b2s])
    task_baseline_learning.submit(wait_for=[s2g])