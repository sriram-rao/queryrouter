from typing import Any, override

import store


class StoreResolver:
    def __init__(self, stores: set[str]) -> None:
        self.stores: set[str] = stores

    def select(self, query: str, metadata: dict[str, Any]) -> str:
        return ""


class LlmSelector(StoreResolver):
    @override
    def select(self, query: str, metadata: dict[str, Any]) -> str:
        print("Call LLM")
        return store.DUCKDB


class RuleChecker(StoreResolver):
    @override
    def select(self, query: str, metadata: dict[str, Any]) -> str:
        if store.DUCKDB not in self.stores or store.TRINO not in self.stores:
            return "Unsupported"
        print("What rules?")
        return store.TRINO
