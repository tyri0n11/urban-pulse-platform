"""Batch processing package.

Configures stdout logging for all batch modules at import time so that
Prefect subprocess workers emit business log lines to container stdout
(picked up by Promtail → Loki → Grafana).
"""

import logging
import sys

_FMT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(logging.Formatter(_FMT))

for _module in (
    "batch.jobs.bronze_to_silver",
    "batch.jobs.silver_to_gold",
    "batch.jobs.baseline_learning",
    "batch.jobs.rag_indexer",
    "batch.pipeline",
):
    _log = logging.getLogger(_module)
    if not _log.handlers:
        _log.addHandler(_handler)
    _log.setLevel(logging.INFO)
    _log.propagate = False
