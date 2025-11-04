import duckdb
from platypus.dremio_client import DremioQueryExecutor
from platypus.salesforce_client import SalesforceQueryExecutor

import soql.soql_queries as soql_queries


def count_parquet_rows(file_path: str) -> int:
    query = f"SELECT COUNT(*) FROM '{file_path}'"
    result = duckdb.execute(query).fetchone()[0]
    return result


file_path = r"output\customers.parquet"
row_count = count_parquet_rows(file_path)
print(f"Row count: {row_count}")
