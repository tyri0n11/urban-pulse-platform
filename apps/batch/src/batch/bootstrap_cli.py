"""CLI: full medallion bootstrap — bronze → silver → gold → baseline.

Usage (inside container):
    python -m batch.bootstrap_cli              # full pipeline
    python -m batch.bootstrap_cli --silver     # silver only
    python -m batch.bootstrap_cli --gold       # gold + baseline only
"""

import argparse
import asyncio
import logging
import sys

from urbanpulse_core.config import settings
from urbanpulse_infra.iceberg import get_iceberg_catalog

from batch.jobs import baseline_learning, bronze_to_silver, silver_to_gold

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("bootstrap_cli")

# Deployments that write to Iceberg — must be paused during bootstrap
_COMPETING_DEPLOYMENTS = ["microbatch-deployment", "hourly-gold-deployment"]


async def _pause_and_cancel() -> None:
    from prefect.client.orchestration import get_client

    async with get_client() as client:
        deployments = await client.read_deployments()
        for dep in deployments:
            if dep.name in _COMPETING_DEPLOYMENTS:
                await client.pause_deployment(dep.id)
                logger.info("Paused deployment: %s", dep.name)


async def _resume_deployments() -> None:
    from prefect.client.orchestration import get_client
    async with get_client() as client:
        deployments = await client.read_deployments()
        for dep in deployments:
            if dep.name in _COMPETING_DEPLOYMENTS:
                await client.resume_deployment(dep.id)
                logger.info("Resumed deployment: %s", dep.name)


def _pause_competing_flows() -> None:
    asyncio.run(_pause_and_cancel())


def _resume_competing_flows() -> None:
    asyncio.run(_resume_deployments())


def _bootstrap_silver() -> int:
    catalog = get_iceberg_catalog(
        catalog_uri=settings.iceberg_catalog_uri,
        minio_endpoint=settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
    )
    rows = bronze_to_silver.bootstrap(catalog=catalog)
    logger.info("Silver bootstrap: %d rows written", rows)
    return rows


def _bootstrap_gold() -> int:
    rows = silver_to_gold.bootstrap()
    logger.info("Gold bootstrap: %d rows written", rows)
    return rows


def _bootstrap_baseline() -> int:
    rows = baseline_learning.run()
    logger.info("Baseline bootstrap: %d rows written", rows)
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Medallion bootstrap CLI")
    parser.add_argument("--silver", action="store_true", help="Only bootstrap silver")
    parser.add_argument("--gold", action="store_true", help="Only bootstrap gold + baseline")
    args = parser.parse_args()

    run_silver = args.silver or (not args.silver and not args.gold)
    run_gold = args.gold or (not args.silver and not args.gold)

    _pause_competing_flows()
    try:
        if run_silver:
            silver_rows = _bootstrap_silver()
            if silver_rows == 0:
                logger.warning("0 silver rows — check bronze data")

        if run_gold:
            gold_rows = _bootstrap_gold()
            if gold_rows == 0:
                logger.warning("0 gold rows — check silver data")
            else:
                _bootstrap_baseline()

        logger.info("Bootstrap complete.")
    finally:
        _resume_competing_flows()

    sys.exit(0)


if __name__ == "__main__":
    main()
