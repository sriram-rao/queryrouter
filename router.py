"""Query router system"""

import experiment
import resolver
import settings
from store import DUCKDB, TRINO, DuckDbConnector, TrinoConnector

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
        {"api_key": settings.claude_api_key},
    )
    metadata = {"schemata": schemata, "row_counts": row_counts}
    print(f"Route picked: {route_picker.select(query, metadata)}")


def start_experiment():
    experiment.run_experiment(config)


if __name__ == "__main__":
    # experiment.run_experiment(config)
    # resolve_route(settings.queries[4])
    start_experiment()
