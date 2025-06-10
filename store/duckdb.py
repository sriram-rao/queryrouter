"""Module for connectors to data stores"""

from typing import Any, override

import duckdb

import settings
from store.connector import DUCKDB, StoreConnector


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
