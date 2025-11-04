"""
This module provides the DremioQueryExecutor class, which facilitates executing SQL queries
against a Dremio database and saving the results in various formats. The class uses a Dremio
client to fetch data and supports output formats such as CSV, Parquet, DuckDB, and DataFrames
(Pandas and Polars).

Classes:
    DremioQueryExecutor: A class to execute SQL queries and save the results.

Usage Example:
    from platypus.dremio_client.query_executor import DremioQueryExecutor
    from platypus.dremio_client.client import DremioDatabase

    # Create a DremioDatabase instance (assumes environment variables are set)
    db = DremioDatabase()

    # Instantiate the query executor
    executor = DremioQueryExecutor(db)

    # Execute a query and save the result as a CSV file
    executor.execute_and_save("SELECT * FROM your_table", table_name="output_table", output_format="csv", output_path="output")

    # Execute multiple queries with configurations
    queries = ["SELECT * FROM table1", "SELECT * FROM table2"]
    configs = [
        {"output_format": "csv", "output_filename": "table1_data"},
        {"output_format": "parquet", "output_filename": "table2_data"}
    ]
    executor.execute_queries_with_configs(queries, configs)
"""

import os

import dotenv
import duckdb
import pandas as pd
import polars as pl
from dotenv import load_dotenv

from platypus.dremio_client.client import DremioDatabase
from platypus.dremio_client.data_fetcher import DremioDataFetcher
from platypus.dremio_client.data_saver import DataSaver


class DremioQueryExecutor:
    def __init__(self, env_path=None) -> None:

        if env_path:
            dotenv.load_dotenv(env_path)
        else:
            load_dotenv()

        self.data_saver = DataSaver()
        self.data_fetcher = DremioDataFetcher()

    def execute_and_save(
        self,
        query: str,
        table_name: str = None,
        output_format: str = None,
        output_path: str = None,
    ) -> pd.DataFrame | pl.DataFrame:
        """
        Executes a query and either saves the result to the specified format and path or returns the DataFrame.

        Args:
            query (str): The SQL query to execute.
            table_name (str, optional): The name of the table/file. Required if saving to a file.
            output_format (str, optional): The format to save the data in ('csv', 'parquet', 'duckdb', 'dataframe', 'polars').
            output_path (str, optional): The path to save the file. Defaults to a local output directory.

        Returns:
            pd.DataFrame or pl.DataFrame: The result of the query as a DataFrame.
        """

        output_format = output_format or "dataframe"

        # Set path only if we are writing to disk
        full_output_path = (
            os.path.join(output_path or self.data_saver.default_output_path, f"{table_name}.{output_format}")
            if output_format not in ("dataframe", "polars")
            else None
        )

        # Fetch the data
        df = self.data_fetcher.fetch_and_clean_data(
            query=query,
            chunk_size=10000,
            full_output_path=full_output_path,
            output_format=output_format,
        )

        # Match on output format
        match output_format:
            case "dataframe" | "polars":
                return df  # No save, just return

            case "csv":
                match df:
                    case pd.DataFrame():
                        df.to_csv(full_output_path, index=False, encoding="utf-8", errors="replace")
                    case pl.DataFrame():
                        df.write_csv(full_output_path)

            case "parquet":
                match df:
                    case pd.DataFrame():
                        df.to_parquet(full_output_path, index=False, engine="pyarrow")
                    case pl.DataFrame():
                        df.write_parquet(full_output_path, use_pyarrow=True)

            case "duckdb":
                if os.path.exists(full_output_path):
                    os.remove(full_output_path)
                con = duckdb.connect(full_output_path)
                con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
                con.close()

            case _:
                raise ValueError(
                    f"Unsupported output_format: {output_format}", " Contact DSA to add support for additional types"
                )

        return df

    def execute_queries_with_configs(self, queries: list[str], output_configs: list[dict]) -> dict:
        """
        Executes multiple queries and saves the results based on the provided configurations.

        Args:
            queries (list[str]): List of SQL queries to execute.
            output_configs (list[dict]): List of output configurations. Each configuration should be a dictionary with keys:
                - 'output_format': The format to save the data ('csv', 'parquet', 'duckdb', 'dataframe', 'polars').
                - 'output_path': The path to save the output file.
                - 'output_filename': The name of the output file (without extension).

        Returns:
            dict: A dictionary of DataFrames if the output format is 'dataframe' or 'polars'.


        Usage Example:
        from platypus.dremio_client.query_executor import DremioQueryExecutor

        # Create a DremioDatabase instance (assumes environment variables are set)

        # Instantiate the query executor
        executor = DremioQueryExecutor()

        # Execute a query and save the result as a CSV file
        executor.execute_and_save("SELECT * FROM your_table", table_name="output_table", output_format="csv", output_path="output")

        # Execute multiple queries with configurations
        queries = ["SELECT * FROM table1", "SELECT * FROM table2"]
        configs = [
            {"output_format": "csv", "output_filename": "table1_data"},
            {"output_format": "parquet", "output_filename": "table2_data"}
        ]
        executor.execute_queries_with_configs(queries, configs)
        """
        dataframes = {}

        for query, config in zip(queries, output_configs):
            output_format = config.get("output_format", "dataframe")
            output_path = config.get("output_path", "output")
            output_filename = config.get("output_filename", "output_file")

            if output_format in ["dataframe", "polars"]:
                df = self.execute_and_save(query, output_filename, output_format, output_path)
                dataframes[output_filename] = df
            else:
                self.execute_and_save(query, output_filename, output_format, output_path)

        return dataframes
