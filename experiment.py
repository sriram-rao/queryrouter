"""Experimenting with the query routers"""

import time
from datetime import datetime
from pathlib import Path
from typing import Any

from resolver import LlmSelector, RuleChecker
from store.connector import ATHENA, DUCKDB, TRINO, StoreConnector

store_types = [DUCKDB, TRINO, ATHENA]
resolver_types = [LlmSelector, RuleChecker]
COMMA = ","
NEWLINE = "\n"


def get_run_time(query: str, store: StoreConnector) -> float:
    _ = store.connect()
    start: float = time.time()
    result = store.run_query(query)
    print(result)
    duration = time.time() - start
    return result["Statistics"]["TotalExecutionTimeInMillis"] / 1000


def get_run_times(queries: list[str], repo: StoreConnector) -> list[float]:
    times = []
    for query in queries:
        times.append(get_run_time(query, repo))
    return times


def compare(config: list[dict[str, Any]]) -> list[list[float]]:
    measures = []
    for test_set in config:
        run_times = get_run_times(test_set["queries"], test_set["store"])
        run_times.insert(0, test_set["name"])
        measures.append(run_times)
    return measures


def run_experiment(
    config: list[dict[str, Any]],
    attempts: int = 2,
    delimiter: str = COMMA,
    output_path: str = "./debug/",
) -> None:
    file_path = Path(output_path) / f"measures_{get_time_string()}.csv"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    for i in range(0, attempts):
        result = compare(config)
        print(result)
        with file_path.open("a+") as file:
            lines = [
                delimiter.join([str(i)] + [str(value) for value in store_row])
                for store_row in result
            ]
            file.write(NEWLINE.join(lines) + "\n")


def get_time_string() -> str:
    return datetime.now().strftime("%Y-%m-%d_%H-%M")
