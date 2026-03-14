"""Orchestrates ingestion jobs across all data sources."""

import json
import logging
import time
from pathlib import Path

from urbanpulse_core.config import settings
from ingestion.publishers import Publisher
from ingestion.sources.vietmap import fetch_route

logger = logging.getLogger(__name__)

_INTER_REQUEST_DELAY_S = 10


def _load_routes() -> list[dict]:
    with open(Path(settings.routes_file)) as f:
        return json.load(f)


def run_once(publisher: Publisher, api_key: str) -> None:
    """Fetch all routes sequentially (10 s delay between calls) and publish each."""
    routes = _load_routes()
    total = len(routes)
    logger.info(
        "Starting crawl cycle: %d routes, %ds inter-request delay",
        total,
        _INTER_REQUEST_DELAY_S,
    )

    for i, route in enumerate(routes):
        try:
            obs = fetch_route(
                route_id=route["route_id"],
                origin=route["origin"],
                destination=route["destination"],
                origin_anchor=route["origin_anchor"],
                destination_anchor=route["destination_anchor"],
                api_key=api_key,
            )
            publisher.publish(obs)
            logger.info(
                "[%d/%d] %s → %s  dist=%.1f km  dur=%.1f min",
                i + 1,
                total,
                route["origin"],
                route["destination"],
                obs.distance_meters / 1000,
                obs.duration_minutes,
            )
        except Exception:
            logger.exception("Failed to fetch route %s", route["route_id"])

        if i < total - 1:
            time.sleep(_INTER_REQUEST_DELAY_S)

    logger.info("Crawl cycle complete.")
