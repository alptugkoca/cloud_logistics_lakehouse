from src.orchestration.runner import run_modules


ALL_MODULES = [
    # Bronze
    "src.bronze.batch_ingest",
    "src.bronze.events_ingest",

    # Silver
    "src.silver.transform_batch",
    "src.silver.transform_events",

    # Gold - batch branch
    "src.gold.logistics_kpi",
    "src.gold.route_performance",
    "src.gold.cost_anomaly",

    # Gold - events branch
    "src.gold.funnel_kpi",
    "src.gold.incidents_kpi",
]


def main() -> None:
    run_modules(ALL_MODULES, "FULL PIPELINE")


if __name__ == "__main__":
    main()