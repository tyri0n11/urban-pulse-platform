"""Publisher implementations for ingestion events."""

import json
from typing import Protocol

from urbanpulse_core.models.traffic import TrafficRouteObservation

TRAFFIC_TOPIC = "traffic-route-bronze"


class Publisher(Protocol):
    def publish(self, observation: TrafficRouteObservation) -> None: ...
    def close(self) -> None: ...


class StdoutPublisher:
    """DRY_RUN publisher — emits newline-delimited JSON to stdout. No Kafka required."""

    def publish(self, observation: TrafficRouteObservation) -> None:
        line = json.dumps(
            {
                "topic": TRAFFIC_TOPIC,
                "key": observation.route_id,
                "value": json.loads(observation.model_dump_json()),
            },
            ensure_ascii=False,
        )
        print(line, flush=True)

    def close(self) -> None:
        pass
