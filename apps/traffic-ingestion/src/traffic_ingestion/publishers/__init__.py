"""Publisher implementations for traffic ingestion events."""

import json
from typing import Protocol

from urbanpulse_core.models.traffic import VietmapRawEnvelope

TRAFFIC_TOPIC = "vietmap-raw"


class Publisher(Protocol):
    def publish(self, envelope: VietmapRawEnvelope) -> None: ...
    def close(self) -> None: ...


class StdoutPublisher:
    """DRY_RUN publisher — emits newline-delimited JSON to stdout. No Kafka required."""

    def publish(self, envelope: VietmapRawEnvelope) -> None:
        line = json.dumps(
            {
                "topic": TRAFFIC_TOPIC,
                "key": envelope.route_id,
                "value": json.loads(envelope.model_dump_json()),
            },
            ensure_ascii=False,
        )
        print(line, flush=True)

    def close(self) -> None:
        pass
