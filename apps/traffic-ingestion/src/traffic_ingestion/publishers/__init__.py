"""Publisher implementations for traffic ingestion events."""

import json
from datetime import datetime
from typing import Any, Protocol

TRAFFIC_TOPIC = "vietmap-raw"


class Publisher(Protocol):
    def publish(
        self,
        route_id: str,
        polled_at_ms: int,
        timestamp_utc: datetime,
        raw_response: dict[str, Any],
    ) -> None: ...
    def close(self) -> None: ...


class StdoutPublisher:
    """DRY_RUN publisher — emits newline-delimited JSON to stdout."""

    def publish(
        self,
        route_id: str,
        polled_at_ms: int,
        timestamp_utc: datetime,
        raw_response: dict[str, Any],
    ) -> None:
        print(json.dumps({
            "topic": TRAFFIC_TOPIC,
            "key": route_id,
            "headers": {
                "route_id": route_id,
                "polled_at_ms": polled_at_ms,
                "timestamp_utc": timestamp_utc.isoformat(),
            },
            "value": raw_response,
        }, ensure_ascii=False), flush=True)

    def close(self) -> None:
        pass
