"""CLI: full medallion bootstrap — bronze → silver → gold → baseline.

Usage (inside container):
    python -m batch.bootstrap_cli              # full pipeline
    python -m batch.bootstrap_cli --silver     # silver only
    python -m batch.bootstrap_cli --gold       # gold + baseline only
"""

import argparse
import logging
import sys

from pyiceberg.exceptions import NoSuchTableError

from urbanpulse_core.config import settings
from urbanpulse_infra.iceberg import get_iceberg_catalog

from batch.jobs import baseline_learning, bronze_to_silver, silver_to_gold

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("bootstrap_cli")

_SILVER_TABLE = "silver.traffic_route"


def _bootstrap_silver() -> int:
    """Drop and re-create silver from all bronze data."""
    catalog = get_iceberg_catalog(
        catalog_uri=settings.iceberg_catalog_uri,
        minio_endpoint=settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
    )

    try:
        catalog.drop_table(_SILVER_TABLE)
        logger.info("Dropped table %s", _SILVER_TABLE)
    except NoSuchTableError:
        logger.info("Table %s does not exist — nothing to drop", _SILVER_TABLE)

    rows = bronze_to_silver.bootstrap(catalog=catalog)
    logger.info("Silver bootstrap: %d rows written", rows)
    return rows


def _bootstrap_gold() -> int:
    """Full re-aggregate: all silver → gold hourly."""
    rows = silver_to_gold.bootstrap()
    logger.info("Gold bootstrap: %d rows written", rows)
    return rows


def _bootstrap_baseline() -> int:
    """Recompute baselines from all gold data."""
    rows = baseline_learning.run()
    logger.info("Baseline bootstrap: %d rows written", rows)
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Medallion bootstrap CLI")
    parser.add_argument("--silver", action="store_true", help="Only bootstrap silver")
    parser.add_argument("--gold", action="store_true", help="Only bootstrap gold + baseline")
    args = parser.parse_args()

    # If no flag specified, run everything
    run_silver = args.silver or (not args.silver and not args.gold)
    run_gold = args.gold or (not args.silver and not args.gold)

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
    sys.exit(0)


if __name__ == "__main__":
    main()
