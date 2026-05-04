import subprocess
import sys
import time
from typing import List


def run_modules(modules: List[str], group_name: str) -> None:
    start_time = time.time()

    print(f"\n{'=' * 60}")
    print(f"Starting: {group_name}")
    print(f"{'=' * 60}")

    for module in modules:
        print(f"\nRunning {module} ...")
        module_start = time.time()

        result = subprocess.run(
            [sys.executable, "-m", module],
            check=False
        )

        duration = time.time() - module_start

        if result.returncode != 0:
            print(f"\nFailed: {module}")
            print(f"Duration before failure: {duration:.2f} sec")
            raise RuntimeError(f"Pipeline stopped because {module} failed.")

        print(f"Done: {module} ({duration:.2f} sec)")

    total_duration = time.time() - start_time

    print(f"\n{'=' * 60}")
    print(f"Completed: {group_name}")
    print(f"Total duration: {total_duration:.2f} sec")
    print(f"{'=' * 60}\n")