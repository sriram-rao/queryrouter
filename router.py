"""Query router system"""

import experiment
from store import DuckDbConnector, TrinoConnector

config = [
    {
        "name": DuckDbConnector.name,
        "store": DuckDbConnector({"data_file": "tpch.duckdb"}),
        "queries": ["show tables", "select * from customer limit 5"],
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
        "queries": [
            "show tables",
            "select * from iceberg.tpch.customer limit 5",
        ],
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


if __name__ == "__main__":
    experiment.run_experiment(config)
