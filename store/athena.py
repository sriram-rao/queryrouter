import time
from typing import Any, override

import boto3

from store.connector import StoreConnector


class AthenaConnector(StoreConnector):
    @override
    def connect(self):
        self.connection = boto3.client(
            "athena",
            aws_access_key_id=self.config["aws_access_id"],
            aws_secret_access_key=self.config["aws_secret_key"],
            region_name=self.config["aws_region"],
        )
        return self

    @override
    def run_query(self, query: str) -> Any:
        # response = self.connection.list_data_catalogs()
        query = query.replace("iceberg.", "")
        query_id = self.start_query(query)
        # print(f"Query submitted with ID: {query_id}")
        statistics = self.poll_query(query_id)
        # print(f"Statistics: {statistics}")
        result = self.fetch_results(query_id)
        # print(f"Result: {result}")
        return {"Rows": result, "Statistics": statistics}

    def start_query(self, query: str) -> str:
        response = self.connection.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                "Catalog": "AwsDataCatalog",
                "Database": "tpch-athena",
            },
            ResultConfiguration={
                "OutputLocation": "s3://store-clickhouse/tpch-athena/query-results/"
            },
            ResultReuseConfiguration={
                "ResultReuseByAgeConfiguration": {"Enabled": False}
            },
        )
        return response["QueryExecutionId"]

    def poll_query(self, query_id: str) -> dict[str, int]:
        statistics = {}
        completed = False
        while not completed:
            # poll
            time.sleep(5)  # in seconds
            execution = self.connection.get_query_execution(QueryExecutionId=query_id)
            status = execution["QueryExecution"]["Status"]["State"]
            print(f"Status: {status}")
            completed = status.upper() not in ["QUEUED", "RUNNING"]
            statistics = execution["QueryExecution"]["Statistics"]
        # print(f"Query complete. Execution statistics:\n{statistics}")
        return statistics

    def fetch_results(self, query_id: str) -> Any:
        response = self.connection.get_query_results(QueryExecutionId=query_id)
        return response["ResultSet"]["Rows"]
