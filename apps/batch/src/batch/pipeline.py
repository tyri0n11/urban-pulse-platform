"""Defines the medallion batch pipeline stages and execution graph."""

from datetime import datetime, timezone

import httpx
from prefect import flow, get_run_logger, task

from batch.jobs import (
    alerter,
    baseline_learning,
    bronze_to_silver,
    bronze_to_silver_weather,
    rag_indexer,
    silver_to_gold,
    silver_to_gold_weather,
    weather_bootstrap,
)

_ML_SERVICE_URL = "http://ml-service:8000"


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(name="microbatch-bronze-to-silver", retries=3, retry_delay_seconds=60)  # type: ignore[untyped-decorator]
def task_microbatch_bronze_to_silver() -> int:
    log = get_run_logger()
    try:
        log.info("Starting microbatch: bronze → silver (traffic)")
        rows = bronze_to_silver.microbatch()
        log.info("microbatch bronze→silver done: %d rows", rows)
        return rows
    except Exception:
        log.exception("microbatch bronze→silver failed")
        raise


@task(name="microbatch-weather-bronze-to-silver", retries=3, retry_delay_seconds=60)  # type: ignore[untyped-decorator]
def task_microbatch_weather_bronze_to_silver() -> int:
    log = get_run_logger()
    try:
        log.info("Starting microbatch: bronze → silver (weather)")
        rows = bronze_to_silver_weather.microbatch()
        log.info("microbatch weather bronze→silver done: %d rows", rows)
        return rows
    except Exception:
        log.exception("microbatch weather bronze→silver failed")
        raise


@task(name="weather-silver-to-gold", retries=3, retry_delay_seconds=60)  # type: ignore[untyped-decorator]
def task_weather_silver_to_gold() -> int:
    log = get_run_logger()
    try:
        log.info("Starting weather silver → gold aggregation")
        rows = silver_to_gold_weather.run()
        log.info("weather silver→gold done: %d rows", rows)
        return rows
    except Exception:
        log.exception("weather silver→gold failed")
        raise


@task(name="bootstrap-weather-bronze", retries=2, retry_delay_seconds=60)  # type: ignore[untyped-decorator]
def task_bootstrap_weather_bronze(from_date: str, to_date: str) -> int:
    log = get_run_logger()
    try:
        log.info("Starting weather bronze backfill: %s → %s", from_date, to_date)
        rows = weather_bootstrap.backfill_to_bronze(from_date, to_date)
        log.info("weather bronze backfill done: %d records written", rows)
        return rows
    except Exception:
        log.exception("weather bronze backfill failed (from=%s to=%s)", from_date, to_date)
        raise


@task(name="bootstrap-weather-silver", retries=3, retry_delay_seconds=60)  # type: ignore[untyped-decorator]
def task_bootstrap_weather_silver() -> int:
    log = get_run_logger()
    try:
        log.info("Starting weather bootstrap: bronze → silver")
        rows = bronze_to_silver_weather.bootstrap()
        log.info("weather bootstrap bronze→silver done: %d rows", rows)
        return rows
    except Exception:
        log.exception("weather bootstrap bronze→silver failed")
        raise


@task(name="bootstrap-weather-gold", retries=3, retry_delay_seconds=60)  # type: ignore[untyped-decorator]
def task_bootstrap_weather_gold() -> int:
    log = get_run_logger()
    try:
        log.info("Starting weather bootstrap: silver → gold")
        rows = silver_to_gold_weather.bootstrap()
        log.info("weather bootstrap silver→gold done: %d rows", rows)
        return rows
    except Exception:
        log.exception("weather bootstrap silver→gold failed")
        raise


@task(name="silver-to-gold", retries=3, retry_delay_seconds=60)  # type: ignore[untyped-decorator]
def task_silver_to_gold() -> int:
    log = get_run_logger()
    try:
        log.info("Starting silver → gold aggregation (traffic)")
        rows = silver_to_gold.run()
        log.info("silver→gold done: %d rows", rows)
        return rows
    except Exception:
        log.exception("silver→gold failed")
        raise


@task(name="baseline-learning", retries=3, retry_delay_seconds=60)  # type: ignore[untyped-decorator]
def task_baseline_learning() -> int:
    log = get_run_logger()
    try:
        log.info("Starting baseline learning")
        rows = baseline_learning.run()
        log.info("baseline learning done: %d routes processed", rows)
        return rows
    except Exception:
        log.exception("baseline learning failed")
        raise


@task(name="index-rag", retries=2, retry_delay_seconds=30)  # type: ignore[untyped-decorator]
def task_index_rag(catalog: object, index_patterns: bool = False) -> dict[str, int]:
    log = get_run_logger()
    try:
        log.info("Starting RAG indexing (index_patterns=%s)", index_patterns)
        result = rag_indexer.run(catalog, index_patterns=index_patterns)  # type: ignore[return-value]
        log.info("RAG indexing done: %s", result)
        return result  # type: ignore[return-value]
    except Exception:
        log.exception("RAG indexing failed")
        raise


@task(name="check-and-alert", retries=2, retry_delay_seconds=30)  # type: ignore[untyped-decorator]
def task_check_and_alert() -> int:
    log = get_run_logger()
    try:
        log.info("Checking anomalies and sending alerts")
        sent = alerter.run()
        log.info("alert check done: %d alerts sent", sent)
        return sent
    except Exception:
        log.exception("alert check failed")
        raise


@task(name="trigger-ml-retrain", retries=2, retry_delay_seconds=30)  # type: ignore[untyped-decorator]
def task_trigger_ml_retrain() -> dict[str, object]:
    log = get_run_logger()
    try:
        log.info("Triggering ML retrain at %s", _ML_SERVICE_URL)
        resp = httpx.post(f"{_ML_SERVICE_URL}/train", timeout=300)
        resp.raise_for_status()
        result = resp.json()
        log.info("ML retrain done: status=%s run_id=%s", result.get("status"), result.get("run_id"))
        return result  # type: ignore[return-value]
    except Exception:
        log.exception("ML retrain trigger failed")
        raise


# ---------------------------------------------------------------------------
# Flows
# ---------------------------------------------------------------------------

@flow(name="microbatch", log_prints=True)  # type: ignore[untyped-decorator]
def microbatch() -> None:
    """Fast-path microbatch: promote new bronze records to silver.

    Runs every 5 min so the silver layer stays close to real-time
    while keeping each DuckDB scan small. Processes both traffic and weather.
    """
    task_microbatch_bronze_to_silver()
    task_microbatch_weather_bronze_to_silver()


@flow(name="hourly-gold", log_prints=True)  # type: ignore[untyped-decorator]
def hourly_gold() -> None:
    """Hourly aggregation: silver → gold + RAG index update.

    After gold is updated, index recent anomaly events into ChromaDB
    so the LLM explanation layer always has fresh historical context.
    """
    from urbanpulse_infra.iceberg import get_iceberg_catalog
    from urbanpulse_core.config import settings
    gold_rows = task_silver_to_gold()
    weather_gold_rows = task_weather_silver_to_gold()
    catalog = get_iceberg_catalog(
        catalog_uri=settings.iceberg_catalog_uri,
        minio_endpoint=settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
    )
    task_index_rag(catalog, index_patterns=False, wait_for=[gold_rows, weather_gold_rows])


@flow(name="retrain", log_prints=True)  # type: ignore[untyped-decorator]
def retrain() -> None:
    """Periodic baseline recompute + model retrain + RAG pattern re-index.

    Runs every 6 hours. Also re-indexes traffic patterns into ChromaDB
    (full gold scan) since retrain already does this scan anyway.
    """
    from urbanpulse_infra.iceberg import get_iceberg_catalog
    from urbanpulse_core.config import settings
    catalog = get_iceberg_catalog(
        catalog_uri=settings.iceberg_catalog_uri,
        minio_endpoint=settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
    )
    baseline_rows = task_baseline_learning()
    task_trigger_ml_retrain(wait_for=[baseline_rows])
    # Re-index traffic patterns with full gold scan (runs alongside retrain)
    task_index_rag(catalog, index_patterns=True)


@flow(name="alert", log_prints=True)  # type: ignore[untyped-decorator]
def alert() -> None:
    """Check for anomalies and send Telegram alerts every 5 minutes.

    Only fires for IForest-confirmed signals (BOTH or IFOREST).
    Deduplicates via anomaly_alert_log table — 30-minute cooldown per route.
    """
    task_check_and_alert()


@flow(name="rag-index", log_prints=True)  # type: ignore[untyped-decorator]
def rag_index(index_patterns: bool = True) -> None:
    """Manual RAG index trigger — indexes anomaly events + traffic patterns.

    Run once after bootstrap, or whenever you want to force a full re-index.
    During normal operation, indexing happens automatically via hourly-gold
    (anomaly events) and retrain (traffic patterns).
    """
    from urbanpulse_infra.iceberg import get_iceberg_catalog
    from urbanpulse_core.config import settings
    catalog = get_iceberg_catalog(
        catalog_uri=settings.iceberg_catalog_uri,
        minio_endpoint=settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
    )
    task_index_rag(catalog, index_patterns=index_patterns)


# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------

@task(name="bootstrap-traffic-silver", retries=3, retry_delay_seconds=60)  # type: ignore[untyped-decorator]
def bootstrap_traffic_silver() -> int:
    log = get_run_logger()
    try:
        log.info("Starting bootstrap: all bronze → silver (traffic)")
        rows = bronze_to_silver.bootstrap()
        log.info("bootstrap traffic silver done: %d rows", rows)
        return rows
    except Exception:
        log.exception("bootstrap traffic silver failed")
        raise


@task(name="bootstrap-silver-to-gold", retries=3, retry_delay_seconds=60)  # type: ignore[untyped-decorator]
def bootstrap_silver_to_gold() -> int:
    log = get_run_logger()
    try:
        log.info("Starting bootstrap: all silver → gold (traffic)")
        rows = silver_to_gold.bootstrap()
        log.info("bootstrap silver→gold done: %d rows", rows)
        return rows
    except Exception:
        log.exception("bootstrap silver→gold failed")
        raise


@task(name="bootstrap-baseline-learning", retries=3, retry_delay_seconds=60)  # type: ignore[untyped-decorator]
def bootstrap_baseline_learning() -> int:
    log = get_run_logger()
    try:
        log.info("Starting bootstrap: baseline learning")
        rows = baseline_learning.run()
        log.info("bootstrap baseline learning done: %d routes", rows)
        return rows
    except Exception:
        log.exception("bootstrap baseline learning failed")
        raise


@flow(name="bootstrap", log_prints=True)  # type: ignore[untyped-decorator]
def bootstrap() -> None:
    """Full medallion bootstrap: bronze → silver → gold → baseline (traffic + weather).

    Runs each stage sequentially with explicit dependencies.
    """
    log = get_run_logger()
    log.info("Starting full medallion bootstrap (traffic)")
    silver_rows = bootstrap_traffic_silver()
    gold_rows = bootstrap_silver_to_gold(wait_for=[silver_rows])
    bootstrap_baseline_learning(wait_for=[gold_rows])
    log.info("Full medallion bootstrap complete")


@flow(name="weather-bootstrap", log_prints=True)  # type: ignore[untyped-decorator]
def weather_bootstrap_flow(
    from_date: str = "2026-01-01",
    to_date: str | None = None,
) -> None:
    """Backfill weather history: Open-Meteo archive → bronze → silver → gold.

    Fetches hourly weather from the Open-Meteo archive API and writes it through
    the full medallion pipeline. Safe to re-run — bronze writes are idempotent
    (deterministic filenames) and silver deduplicates on (location_id, hour_utc).

    Args:
        from_date: ISO date string, e.g. "2026-01-01"
        to_date:   ISO date string (defaults to today)
    """
    log = get_run_logger()
    resolved_to = to_date or datetime.now(timezone.utc).date().isoformat()
    log.info("Starting weather bootstrap: %s → %s", from_date, resolved_to)

    bronze_rows = task_bootstrap_weather_bronze(from_date, resolved_to)
    silver_rows = task_bootstrap_weather_silver(wait_for=[bronze_rows])
    task_bootstrap_weather_gold(wait_for=[silver_rows])
    log.info("Weather bootstrap complete")


# ---------------------------------------------------------------------------
# Backfill
# ---------------------------------------------------------------------------

@task(name="backfill-traffic-silver", retries=2, retry_delay_seconds=30)  # type: ignore[untyped-decorator]
def task_backfill_traffic_silver(from_dt: datetime, to_dt: datetime) -> int:
    log = get_run_logger()
    try:
        log.info("Starting backfill: bronze → silver from %s to %s", from_dt, to_dt)
        rows = bronze_to_silver.backfill(from_dt=from_dt, to_dt=to_dt)
        log.info("backfill bronze→silver done: %d rows", rows)
        return rows
    except Exception:
        log.exception("backfill bronze→silver failed (from=%s to=%s)", from_dt, to_dt)
        raise


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
    log = get_run_logger()
    resolved_to = to_dt or datetime.now(timezone.utc)
    log.info("Starting backfill: silver from %s to %s", from_dt, resolved_to)
    task_backfill_traffic_silver(from_dt=from_dt, to_dt=resolved_to)
    log.info("Backfill complete")
