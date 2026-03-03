import json
import random
import time
from datetime import datetime, timezone
from typing import Dict, List, Tuple

from google.cloud import pubsub_v1

PROJECT = "fluted-cogency-403608"
TOPIC = "cab-telemetry"
MOVE_PROBABILITY = 0.8
CYCLE_SLEEP_SECONDS = 10


def initial_cab_state(count: int = 50) -> Tuple[List[str], Dict[str, Dict[str, float]]]:
    cab_ids = [f"CAB{i:03d}" for i in range(1, count + 1)]
    cab_states = {
        cab: {"lat": 40.0 + random.random() * 0.1, "lon": -74.0 + random.random() * 0.1}
        for cab in cab_ids
    }
    return cab_ids, cab_states


def publish_mock_data() -> None:
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT, TOPIC)
    cab_ids, cab_states = initial_cab_state()

    while True:
        for cab in cab_ids:
            state = cab_states[cab]

            if random.random() < MOVE_PROBABILITY:
                state["lat"] += (random.random() - 0.5) * 0.0005
                state["lon"] += (random.random() - 0.5) * 0.0005

            speed = random.choice([0.0, round(random.uniform(5, 60), 1)])
            msg = {
                "cab_id": cab,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "lat": state["lat"],
                "lon": state["lon"],
                "speed": speed,
            }

            publisher.publish(topic_path, json.dumps(msg).encode("utf-8"))
            print(f"Published: {msg}")

        print(f"Cycle complete, sleeping {CYCLE_SLEEP_SECONDS}s before next cycle")
        time.sleep(CYCLE_SLEEP_SECONDS)


if __name__ == "__main__":
    publish_mock_data()
