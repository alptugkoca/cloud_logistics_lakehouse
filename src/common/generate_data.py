import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd


def generate_logistics_data() -> None:
    np.random.seed(42)

    routes = ["NL-DE", "NL-FR", "NL-IT", "DE-FR", "FR-ES"]
    transport_modes = ["Truck", "Rail", "Sea", "Air"]

    data = []

    for _ in range(1000):
        mode = np.random.choice(transport_modes)

        if mode == "Air":
            cost = np.random.uniform(2000, 8000)
        elif mode == "Sea":
            cost = np.random.uniform(1000, 4000)
        elif mode == "Rail":
            cost = np.random.uniform(800, 3000)
        else:  # Truck
            cost = np.random.uniform(500, 2500)

        data.append(
            {
                "route_id": np.random.choice(routes),
                "transport_mode": mode,
                "cost_eur": round(cost, 2),
                "distance_km": np.random.randint(100, 2000),
                "shipment_date": (
                    datetime.now() - timedelta(days=np.random.randint(0, 30))
                ).strftime("%Y-%m-%d"),
            }
        )

    df = pd.DataFrame(data)
    df.to_csv("data/raw/batch/logistics_costs.csv", index=False)

    print("Logistics cost data generated.")


def generate_events_data() -> None:
    event_types = ["page_view", "add_to_cart", "checkout", "purchase"]

    events = []

    for _ in range(2000):
        events.append(
            {
                "user_id": np.random.randint(1, 1000),
                "event_type": np.random.choice(
                    event_types, p=[0.5, 0.2, 0.2, 0.1]
                ),
                "timestamp": (
                    datetime.now() - timedelta(minutes=np.random.randint(0, 1440))
                ).strftime("%Y-%m-%d %H:%M:%S"),
            }
        )

    df = pd.DataFrame(events)
    df.to_csv("data/raw/events/events_data.csv", index=False)

    print("Event data generated.")


def generate_incidents_data() -> None:
    routes = ["NL-DE", "NL-FR", "NL-IT", "DE-FR", "FR-ES"]
    incident_types = ["delay", "damage", "theft", "customs_issue"]

    incidents = []

    for _ in range(500):
        incidents.append(
            {
                "route_id": np.random.choice(routes),
                "incident_type": np.random.choice(
                    incident_types, p=[0.5, 0.2, 0.2, 0.1]
                ),
                "severity": np.random.choice(["low", "medium", "high"]),
                "timestamp": (
                    datetime.now() - timedelta(minutes=np.random.randint(0, 1440))
                ).strftime("%Y-%m-%d %H:%M:%S"),
            }
        )

    df = pd.DataFrame(incidents)
    df.to_csv("data/raw/events/logistics_incidents.csv", index=False)

    print("Incident data generated.")


def run() -> None:
    # Create directories
    os.makedirs("data/raw/batch", exist_ok=True)
    os.makedirs("data/raw/events", exist_ok=True)

    generate_logistics_data()
    generate_events_data()
    generate_incidents_data()

    print("All synthetic data generated successfully.")


if __name__ == "__main__":
    run()