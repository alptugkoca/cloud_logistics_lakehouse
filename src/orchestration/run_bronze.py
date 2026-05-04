from src.orchestration.runner import run_modules


BRONZE_MODULES = [
    "src.bronze.batch_ingest",
    "src.bronze.events_ingest",
]


def main() -> None:
    run_modules(BRONZE_MODULES, "BRONZE")


if __name__ == "__main__":
    main()