from __future__ import annotations

import os
from typing import Dict, List, Optional

import duckdb
import pandas as pd
import pyarrow.parquet as pq
from platypus.dremio_client import DremioQueryExecutor

pd.set_option("display.max_rows", None)  # Set to None to display all rows
pd.set_option("display.max_columns", None)  # Set to None to display all columns

DUCKDB_FILE = r"db\query_log.duckdb"


def get_sql_queries(directory="sql") -> Dict:
    """
    Reads all SQL files in a directory and returns a dictionary where
    the key is the filename (with .sql) and the value is the SQL query.
    :param sql_dir: Directory path containing SQL files.
    :return: Dictionary with filenames as keys and SQL queries as values.
    """
    queries = {}

    # Loop through the directory and read each SQL file

    for filename in os.listdir(directory):
        if filename.endswith(".sql"):
            file_path = os.path.join(directory, filename)
            with open(file_path, "r") as file:
                sql_query = file.read()
                # Store the query content with the filename as the key
                queries[filename] = sql_query
    return queries


def count_parquet_rows(file_path: str) -> int:

    file_path = r"output\small_business_tm_package_customers.parquet"
    row_count = count_parquet_rows(file_path)
    print(f"Row count: {row_count}")

    query = f"SELECT COUNT(*) FROM '{file_path}'"
    result = duckdb.execute(query).fetchone()[0]
    return result


def describe_parquet(file_path) -> None:
    """describe_parquet("example.parquet")"""
    file_size = os.path.getsize(file_path)
    print(f"File Size: {file_size} bytes")

    table = pq.read_table(file_path)
    columns = table.column_names

    print(f"Number of rows: {table.num_rows}")
    print(f"Number of columns: {len(columns)}")

    print("Columns:")
    for column in columns:
        print(column)


def get_duckdb_table_names(database_path: str) -> List[str]:
    """
    tables = get_duckdb_table_names()
    print("Available tables:")
    for i, table in enumerate(tables, 1):
    print(f"{i}. {table}")
    """
    con = duckdb.connect(database_path)
    tables = con.execute("SELECT table_name FROM information_schema.tables").fetchall()
    return [table[0] for table in tables]


def fetch_query_log() -> pd.DataFrame:
    # View full query execution history.
    con = duckdb.connect(DUCKDB_FILE)
    df = con.execute("SELECT * FROM query_log ORDER BY last_run DESC").fetchdf()
    con.close()
    return df


def fetch_query_performance() -> pd.DataFrame:
    # View execution times for successful queries.
    con = duckdb.connect(DUCKDB_FILE)
    df = con.execute("SELECT * FROM query_performance_log ORDER BY last_run DESC").fetchdf()
    con.close()
    return df


def fetch_failed_queries() -> pd.DataFrame:
    # Fetch and display only failed queries from DuckDB.
    con = duckdb.connect(DUCKDB_FILE)
    df = con.execute("SELECT * FROM query_log WHERE status = 'failed' ORDER BY last_run DESC").fetchdf()
    con.close()
    return df


def fetch_successful_queries() -> pd.DataFrame:
    # Fetch and display only successful queries from DuckDB.
    con = duckdb.connect(DUCKDB_FILE)
    df = con.execute("SELECT * FROM query_log WHERE status = 'success' ORDER BY last_run DESC").fetchdf()
    con.close()
    return df


def clear_query_log() -> None:
    # Clear all records from the DuckDB query log (use with caution).
    con = duckdb.connect(DUCKDB_FILE)
    con.execute("DELETE FROM query_log")
    con.close()
    print("✅ Query log cleared successfully.")


def delete_query_log(query_name, last_run) -> None:
    # Delete a specific query from the log based on its name.
    con = duckdb.connect(DUCKDB_FILE)
    con.execute("DELETE FROM query_log WHERE query_name = ? and last_run = ?", (query_name, last_run))
    con.close()
    print(f"✅ Query log entry deleted for: {query_name, last_run}")


def drop_query_log() -> None:
    # Delete a query log from exsitence.
    con = duckdb.connect(DUCKDB_FILE)
    con.execute("DROP TABLE IF EXISTS query_log")
    con.close()
    print(f"✅ Query log erased from existence 😮")


def drop_performance_log() -> None:
    # Delete a performance log from exsitence.
    con = duckdb.connect(DUCKDB_FILE)
    con.execute("DROP TABLE IF EXISTS query_performance_log")
    con.close()
    print(f"✅ Performance log erased from existence 😮")


# %%
# View full query log
# fetch_query_log()
# delete_query_log("bc_listings.sql", "2025-08-12 08:05:06")


# View full performance log
# fetch_query_performance()

# View failed queries
# fetch_failed_queries()

# View successful queries
# fetch_successful_queries()

# Clear entire query log (Use with caution)
# clear_query_log()

# Delete a specific query's record
# delete_query_log("branch_summary.sql")


# # %%
# import duckdb

# DUCKDB_FILE = r"db\query_log.duckdb"

# query = """
# SELECT query_name, avg(execution_time_seconds/60) as avg_min
# FROM query_performance_log
# where query_name in ('contact.sql', 'account.sql')
# GROUP BY query_name
# ORDER BY query_name
# """
# con = duckdb.connect(database=DUCKDB_FILE)
# df = con.execute(query).fetchdf()
# print(df.head())

# con.close()
# # %%

# import duckdb

# DUCKDB_FILE = r"db\query_log.duckdb"

# query = """
# SELECT * FROM query_performance_log order by last_run desc

# """
# con = duckdb.connect(database=DUCKDB_FILE)
# df = con.execute(query).fetchdf()
# print(df.head())

# con.close()
