import json
from typing import Any, override

from anthropic import Anthropic

import store


class StoreResolver:
    def __init__(self, stores: set[str], config: dict[str, str]) -> None:
        self.stores: set[str] = stores
        self.config: dict[str, str] = config

    def select(self, query: str, metadata: dict[str, Any]) -> str:
        return ""


class LlmSelector(StoreResolver):
    @override
    def __init__(self, stores: set[str], config: dict[str, str]) -> None:
        super().__init__(stores, config)
        self.client = Anthropic(api_key=config["api_key"])

    @override
    def select(self, query: str, metadata: dict[str, Any]) -> str:
        print(f"Query: {query}")
        selection: str = ask_anthropic(self.client, query, metadata)
        print(f"Selection: {selection}, {selection.lower()}")
        if selection and selection.lower() in self.stores:
            return selection.lower()
        raise ValueError("Invalid response.")


class RuleChecker(StoreResolver):
    @override
    def select(self, query: str, metadata: dict[str, Any]) -> str:
        if store.DUCKDB not in self.stores or store.TRINO not in self.stores:
            return "Unsupported"
        print("What rules?")
        return store.TRINO


message_content: str = """
You are an expert database engineer tasked with recommending either DuckDB or Trino for executing a specific SQL query based on the given schema, row counts, and the query itself. 
Your goal is to analyze the information provided and make an informed decision on which system would be more suitable for the task.

First, review the schema information:
{{SCHEMA}}

Next, examine the row counts for each table:
{{ROW_COUNTS}}

Now, consider the query that needs to be executed:
{{QUERY}}

Analyze the provided information, taking into account the following factors:
1. The complexity of the schema and relationships between tables
2. The size of the data based on row counts
3. The nature of the query (e.g., joins, aggregations, complex operations)
4. The strengths and weaknesses of DuckDB and Trino
   DuckDB is generally better for:
   - Smaller datasets that can fit in memory
   - Single-node operations
   - Analytical queries on local data

   Trino is generally better for:
   - Large-scale distributed queries
   - Querying data across multiple heterogeneous sources
   - Handling very large datasets that don't fit in memory

Based on your analysis, recommend either DuckDB or Trino for executing this query. Answer in two parts: suggestion and explanation.
Your entire answer must be in the following JSON format:
{
"selection": "[DuckDB/Trino]",
"explanation": "Placeholder text"
}
Your "selection" must always be only one word and never more than one word.
All of your explanation must be in "explanation". If you do not respond as this JSON, my code won't work.
"""


def ask_anthropic(client, query, metadata) -> str:
    content = (
        message_content.replace("{{QUERY}}", query)
        .replace("{{SCHEMA}}", json.dumps(metadata["schemata"]))
        .replace("{{ROW_COUNTS}}", json.dumps(metadata["row_counts"]))
    )
    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=20000,
        temperature=1,
        messages=[
            {
                "role": "user",
                "content": [{"type": "text", "text": content}],
            }
        ],
    )
    print("Text response")
    print(message.content[0].text)
    response = json.loads(message.content[0].text)
    return response["selection"]


def ask_chatgpt(client, query, metadata):
    response = client.chat.completions.create(
        model="o4-mini",
        messages=[
            # {"role": "dev", "content": "answer in only one word. duckdb or trino"},
            {"role": "system", "content": f"query: {query}"},
            {"role": "user", "content": f"schemata: {metadata['schemata']}"},
            {"role": "user", "content": f"table sizes: {metadata['row_counts']}"},
            {
                "role": "user",
                "content": "which mechanism is more suitable here? duckdb or trino",
            },
        ],
    )
    selection: str | none = response.choices[0].message.content
    return selection
