import json
import os
import random
import time
import uuid
from datetime import datetime


OUTPUT_PATH = "data/streaming/events"

EVENT_TYPES = ["page_view", "add_to_cart", "checkout", "purchase"]
INCIDENT_TYPES = ["delay", "damage", "theft", "customs_issue"]
SEVERITY = ["low", "medium", "high"]


def generate_event() -> dict:
    return {
        "user_id": random.randint(1, 1000),
        "event_type": random.choice(EVENT_TYPES),
        "timestamp": datetime.utcnow().isoformat(),
    }


def generate_incident() -> dict:
    return {
        "route_id": random.choice(["NL-DE", "DE-FR", "NL-IT"]),
        "incident_type": random.choice(INCIDENT_TYPES),
        "severity": random.choice(SEVERITY),
        "timestamp": datetime.utcnow().isoformat(),
    }


def run() -> None:
    os.makedirs(OUTPUT_PATH, exist_ok=True)

    while True:
        record_type = random.choice(["event", "incident"])

        if record_type == "event":
            data = generate_event()
            filename = f"event_{uuid.uuid4()}.json"
        else:
            data = generate_incident()
            filename = f"incident_{uuid.uuid4()}.json"

        with open(f"{OUTPUT_PATH}/{filename}", "w", encoding="utf-8") as f:
            json.dump(data, f)

        print(f"Generated: {filename}")

        time.sleep(1)


if __name__ == "__main__":
    run()