from src.orchestration.runner import run_modules


SILVER_MODULES = [
    "src.silver.transform_batch",
    "src.silver.transform_events",
]


def main() -> None:
    run_modules(SILVER_MODULES, "SILVER")


if __name__ == "__main__":
    main()