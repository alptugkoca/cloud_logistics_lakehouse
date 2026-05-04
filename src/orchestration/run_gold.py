from src.orchestration.runner import run_modules


GOLD_MODULES = [
    "src.gold.route_performance",
    "src.gold.logistics_kpi",
    "src.gold.incidents_kpi",
    "src.gold.funnel_kpi",
    "src.gold.cost_anomaly",
]


def main() -> None:
    run_modules(GOLD_MODULES, "GOLD")


if __name__ == "__main__":
    main()