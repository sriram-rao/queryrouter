"""Module for connectors to data stores"""

from typing import Any, override

import duckdb
import pandas as pd
from trino.dbapi import Connection, Cursor, connect

import settings

DUCKDB = "duckdb"
TRINO = "trino"


class StoreConnector:
    """Base class for connectors of different types"""

    name: str = "base"

    def __init__(self, config: dict[str, str]):
        self.config: dict[str, str] = config
        self.connection: Any = None

    def connect(self):
        return self

    def run_query(self, query: str) -> Any:
        return [[]]

    def execute_command(self, command: str) -> int:
        return -1


class TrinoConnector(StoreConnector):
    """Making a Trino Connector"""

    name: str = TRINO

    @override
    def connect(self):
        self.connection = connect(
            host=self.config["host"],
            port=self.config["port"],
            user=self.config["user"],
            catalog=self.config["catalog"],
            schema=self.config["schema"],
        )
        return self

    @override
    def run_query(self, query: str) -> list[list[object]]:
        cursor: Cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor.fetchall()

    @override
    def execute_command(self, command: str) -> int:
        return -1


class DuckDbConnector(StoreConnector):
    """Making a DuckDB connector"""

    name: str = DUCKDB

    @override
    def connect(self):
        self.connection = duckdb.connect(self.config["data_file"])
        return self

    @override
    def run_query(self, query: str) -> Any:
        response = self.connection.sql(query.replace("iceberg.tpch.", ""))
        print(response)
        return response

    def get_tables(self, query: str) -> list[str]:
        query = query.lower()
        return [name for name in settings.tables if name.lower() in query]

    def get_size(self, table: str) -> int:
        count: str = (
            self.run_query(f"SELECT COUNT(*) FROM {table}")
            .to_df()
            .iloc[0]["count_star()"]
        )
        return int(count)

    def get_schema(self, table: str) -> dict[str, str]:
        response: dict[str, str] = (
            self.run_query(f"DESCRIBE {table}").fetchdf().to_dict()
        )
        return response

    def __del__(self):
        if self.connection:
            self.connection.close()
