"""Module for connectors to data stores"""

from typing import Any, override

import duckdb
from trino.dbapi import Connection, Cursor, connect

DUCKDB = "DuckDB"
TRINO = "Trino"


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
        return self.connection.sql(query)

    def __del__(self):
        self.connection.close()
