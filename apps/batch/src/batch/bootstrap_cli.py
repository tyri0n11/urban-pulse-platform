"""CLI: drop silver table and re-run full bootstrap.

Usage (inside container):
    python -m batch.bootstrap_cli
"""

import logging
import sys

from pyiceberg.exceptions import NoSuchTableError

from urbanpulse_core.config import settings
from urbanpulse_infra.iceberg import get_iceberg_catalog

from batch.jobs.bronze_to_silver import bootstrap

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("bootstrap_cli")

_SILVER_TABLE = "silver.traffic_route"


def main() -> None:
    catalog = get_iceberg_catalog(
        catalog_uri=settings.iceberg_catalog_uri,
        minio_endpoint=settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
    )

    # 1. Drop existing silver table
    try:
        catalog.drop_table(_SILVER_TABLE)
        logger.info("Dropped table %s", _SILVER_TABLE)
    except NoSuchTableError:
        logger.info("Table %s does not exist — nothing to drop", _SILVER_TABLE)

    # 2. Run bootstrap (recreates table with new schema + backfills all bronze)
    rows = bootstrap(catalog=catalog)
    if rows > 0:
        logger.info("Bootstrap complete: %d rows written to %s", rows, _SILVER_TABLE)
    else:
        logger.warning("Bootstrap complete but 0 rows written — check bronze data")

    sys.exit(0)


if __name__ == "__main__":
    main()
