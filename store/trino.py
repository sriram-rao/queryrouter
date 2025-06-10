"""Module for connectors to data stores"""

from typing import override

from trino.dbapi import Cursor, connect

from store.connector import TRINO, StoreConnector


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
