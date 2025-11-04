"""
This module provides the SalesforceQueryExecutor class, which facilitates executing SOQL queries
against a Salesforce database and saving the results in various formats. The class uses a Salesforce
client to fetch data and supports output formats such as CSV, Parquet, DuckDB, and DataFrames
(Pandas and Polars).

Classes:
    SalesforceQueryExecutor: A class to execute SOQL queries and save the results.

Usage Example:
    from platypus.salesforce_client.query_executor import SalesforceQueryExecutor
    from soql_queries import account_soql
    queries = [
        account_soql,
        account_soql,
        account_soql,
        account_soql,
        account_soql
    ]

    output_configs = [
        {
            'output_format': 'dataframe',
            'output_filename': 'sf_table1_data'
        },
        {
            'output_format': 'polars',
            'output_filename': 'sf_table2_data'
        },
        {
            'output_format': 'parquet',
            'output_filename': 'sf_table3_data'
        },
        {
            'output_format': 'csv',
            'output_filename': 'sf_table4_data'
        },
        {
            'output_format': 'duckdb',
            'output_filename': 'sf_table5_data'
        }
    ]

    executor = SalesforceQueryExecutor()
    dataframes = executor.execute_queries_with_configs(queries, output_configs)

    df1 = dataframes['sf_table1_data']
    df2 = dataframes['sf_table2_data']

    df1.head()
    df2.head()

    df_polars = pd.read_parquet('output/sf_table3_data.parquet')
    df_csv = pd.read_csv('output/sf_table4_data.csv')

    con = duckdb.connect(f'output/sf_table5_data.duckdb')
    df_duckdb = con.execute('SELECT * FROM sf_table5_data').fetchdf()

    df1.head()
    df2.head()
    df_polars.head()
    df_csv.head()
    df_duckdb.head()
"""

import os
from datetime import datetime

import duckdb
import pandas as pd
import polars as pl

from platypus.salesforce_client.client import SalesforceClient
from platypus.salesforce_client.data_fetcher import SalesforceDataFetcher
from platypus.salesforce_client.data_saver import SalesforceDataSaver


class SalesforceQueryExecutor:
    """
    Executes Salesforce queries and saves the results in various formats.

    This class uses a Salesforce client to execute queries and fetch data. The data can be saved
    in different formats such as CSV, Parquet, or DuckDB, or returned as a DataFrame.

    Attributes:
        client: The Salesforce client used to execute queries.
        data_fetcher: An instance of SalesforceDataFetcher to fetch and clean data.
        data_saver: An instance of SalesforceDataSaver to save data.
        default_output_path: The default directory path where output files are saved.

    Methods:
        execute_and_save(query, output_filename, output_format, output_path):
            Executes a query and saves the result in the specified format and path.
    """

    def __init__(self, env_path=None) -> None:
        self.client = SalesforceClient().get_client(env_path)
        self.data_fetcher = SalesforceDataFetcher(self.client)
        self.data_saver = SalesforceDataSaver()
        self.default_output_path = "output"

    def execute_and_save(
        self, query: str, output_filename: str = None, output_format: str = None, output_path: str = None
    ):
        """
        Executes a Salesforce query and saves the result in the specified format and path.

        Args:
            query (str): The SOQL query to execute.
            output_filename (str): The name of the output file (without extension).
            output_format (str, optional): The format to save the data in ('csv', 'parquet', 'duckdb', 'dataframe', 'polars').
                Defaults to 'dataframe'.
            output_path (str, optional): The directory path to save the file. Defaults to 'output'.

        Returns:
            pd.DataFrame or pl.DataFrame: The result of the query as a DataFrame.
        """
        output_format = output_format or "dataframe"
        output_path = output_path or self.default_output_path

        # Always fetch the data first
        df = self.data_fetcher.fetch_and_clean_data(
            query, output_filename, full_output_path=None, output_format=output_format
        )

        if output_format in ["dataframe", "polars"]:
            return df

        # For file output, construct the full path.
        full_output_path = os.path.join(output_path, f"{output_filename}.{output_format}")

        # Save based on the output format.
        match output_format:
            case "csv":
                if isinstance(df, pd.DataFrame):
                    df.to_csv(full_output_path, index=False, encoding="utf-8", errors="replace")
                elif isinstance(df, pl.DataFrame):
                    df.write_csv(full_output_path)
            case "parquet":
                if isinstance(df, pd.DataFrame):
                    df.to_parquet(full_output_path, index=False, engine="pyarrow")
                elif isinstance(df, pl.DataFrame):
                    df.write_parquet(full_output_path, use_pyarrow=True)
            case "duckdb":
                if os.path.exists(full_output_path):
                    os.remove(full_output_path)
                con = duckdb.connect(full_output_path)
                con.execute(f"CREATE TABLE {output_filename} AS SELECT * FROM df")
                con.close()
            case _:
                raise ValueError(
                    f"Unsupported output_format: {output_format}",
                    "Contact DSA to add support for additional types",
                )
        return df

    def execute_queries_with_configs(self, queries: list[str], output_configs: list[dict]) -> dict:
        """
        Executes multiple queries and saves the results based on the provided configurations.

        Args:
            queries (list[str]): List of SOQL queries to execute.
            output_configs (list[dict]): List of output configurations. Each configuration should be a dictionary with keys:
                - 'output_format': The format to save the data ('csv', 'parquet', 'duckdb', 'polars', 'dataframe').
                - 'output_path': The path to save the output file.
                - 'output_filename': The name of the output file (without extension).

        Returns:
            dict: A dictionary of DataFrames if the output format is 'dataframe'.

        Example:
            queries = [
                account_soql,
                account_soql,
                account_soql,
                account_soql,
                account_soql
            ]

            output_configs = [
                {
                    'output_format': 'dataframe',
                    'output_filename': 'sf_table1_data'
                },
                {
                    'output_format': 'polars',
                    'output_filename': 'sf_table2_data'
                },
                {
                    'output_format': 'parquet',
                    'output_filename': 'sf_table3_data'
                },
                {
                    'output_format': 'csv',
                    'output_filename': 'sf_table4_data'
                },
                {
                    'output_format': 'duckdb',
                    'output_filename': 'sf_table5_data'
                }
            ]

            executor = SalesforceQueryExecutor()
            dataframes = executor.execute_queries_with_configs(queries, output_configs)

            df1 = dataframes['sf_table1_data']
            df2 = dataframes['sf_table2_data']

            df1.head()
            df2.head()

            df_polars = pd.read_parquet('output/sf_table3_data.parquet')
            df_csv = pd.read_csv('output/sf_table4_data.csv')

            con = duckdb.connect(f'output/sf_table5_data.duckdb')
            df_duckdb = con.execute('SELECT * FROM sf_table5_data').fetchdf()

            df1.head()
            df2.head()
            df_polars.head()
            df_csv.head()
            df_duckdb.head()
        """
        dataframes = {}

        for query, config in zip(queries, output_configs):
            output_format = config.get("output_format", "dataframe")
            output_path = config.get("output_path", self.default_output_path)
            output_filename = config.get("output_filename", "output_file")

            if output_format in ["dataframe", "polars"]:
                df = self.execute_and_save(query, output_filename, output_format, output_path)
                dataframes[output_filename] = df
            else:
                self.execute_and_save(query, output_filename, output_format, output_path)

        return dataframes
