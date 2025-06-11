"""Query router system"""

from pathlib import Path

import experiment
import resolver
import settings
from store.athena import AthenaConnector
from store.connector import DUCKDB, TRINO
from store.duckdb import DuckDbConnector
from store.trino import TrinoConnector

config = [
    {
        "name": DuckDbConnector.name,
        "store": DuckDbConnector({"data_file": "tpch.duckdb"}),
        "queries": settings.queries,
    },
    {
        "name": TrinoConnector.name,
        "store": TrinoConnector(
            {
                "host": "3.138.154.75",
                "port": "8080",
                "user": "ec2-user",
                "catalog": "iceberg",
                "schema": "tpch",
            }
        ),
        "queries": settings.queries,
    },
    {
        "name": AthenaConnector.name,
        "store": AthenaConnector(
            {
                "aws_access_id": settings.Keys.aws_access_id,
                "aws_secret_key": settings.Keys.aws_secret_key,
                "aws_region": settings.Keys.aws_region,
            }
        ),
        "queries": settings.queries,
    },
]


def test_duck():
    duck = DuckDbConnector({"data_file": "tpch.duckdb"}).connect()
    result = duck.run_query("show tables;")
    result.show()


def test_trino():
    result = (
        TrinoConnector(
            {
                "host": "3.138.154.75",
                "port": "8080",
                "user": "ec2-user",
                "catalog": "iceberg",
                "schema": "tpch",
            }
        )
        .connect()
        .run_query("show tables")
    )
    print(result)


def test_athena():
    result = (
        AthenaConnector(
            {
                "aws_access_id": settings.Keys.aws_access_id,
                "aws_secret_key": settings.Keys.aws_secret_key,
                "aws_region": settings.Keys.aws_region,
            }
        )
        .connect()
        .run_query("SELECT COUNT(*) FROM iceberg.tpch.lineitem")
    )
    print(result)


def store_compare():
    result = experiment.compare(config)
    print(result)


def resolve_route(query: str):
    metadata_connector = DuckDbConnector({"data_file": "tpch.duckdb"})
    metadata_connector.connect()
    tables = metadata_connector.get_tables(query)
    schemata: dict[str, dict[str, str]] = {}
    for table in tables:
        schemata[table] = metadata_connector.get_schema(query)
    row_counts = {table: metadata_connector.get_size(table) for table in tables}
    route_picker = resolver.LlmSelector(
        {DUCKDB, TRINO},
        {"api_key": settings.Keys.claude_api_key},
    )
    metadata = {"schemata": schemata, "row_counts": row_counts}
    choice = route_picker.select(query, metadata)
    print(f"Route picked: {choice}")
    return choice


def get_predictions(attempts: int = 2, path: str = "./debug/"):
    file_path = Path(path).resolve() / f"selections_{experiment.get_time_string()}.csv"
    file_path.parent.mkdir(exist_ok=True)
    print(f"Number of queries: {len(settings.queries)}")
    for i in range(0, attempts):
        print(f"Attempt {i}")
        attempt_result = []
        for index, query in enumerate(settings.queries):
            start = time.time()
            try:
                choice = resolve_route(query)
                duration = time.time() - start
            except:
                print(f"Attempt: {i}, Query: {index}, received exception")
                duration = 2000
            attempt_result.append([str(i), str(index), str(choice), str(duration)])
        print(f"Writing attempt {i} to file: {attempt_result}")
        lines = [",".join(result) for result in attempt_result]
        with file_path.open("a+") as file:
            file.write("\n".join(lines) + "\n")


def start_experiment():
    experiment.run_experiment(config[-1:])


if __name__ == "__main__":
    # experiment.run_experiment(config)
    # resolve_route(settings.queries[4])
    start_experiment()
