"""Module for connectors to data stores"""

from typing import Any, override

DUCKDB = "duckdb"
TRINO = "trino"
ATHENA = "athena"


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
