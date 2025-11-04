

import pandas as pd
from pathlib import Path
import duckdb
from typing import List
from tableauhyperapi import (
    Connection,
    CreateMode,
    HyperProcess,
    HyperException,
    Inserter,
    SqlType,
    TableDefinition,
    Telemetry,
    NOT_NULLABLE, 
    NULLABLE,
    escape_name,
    escape_string_literal
)


class HyperFileCreator:
    def __init__(self, telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU):
        self.telemetry = telemetry

    def pandas_to_smallest_sqltype(self, df: pd.DataFrame) -> dict:
        """Converts Pandas DataFrame column types to the smallest possible SQL types, handling nulls.
        Also converts datetime columns to date format ('YYYY-MM-DD') and handles NaN values appropriately.

        Args:
            df: A Pandas DataFrame.

        Returns:
            A dictionary where keys are column names and values are the corresponding SQL types.
        """
        columns = {}
        for col_name, dtype in df.dtypes.items():
            all_null = df[col_name].isnull().all()

            if all_null:
                continue

            nullable = df[col_name].isnull().any()

            if pd.api.types.is_integer_dtype(dtype):
                max_value = df[col_name].max()
                min_value = df[col_name].min()
                if min_value >= 0:
                    if max_value <= 255:
                        sql_type = SqlType.small_int()
                    elif max_value <= 65535:
                        sql_type = SqlType.int()
                    else:
                        sql_type = SqlType.big_int()
                else:
                    if min_value >= -32768 and max_value <= 32767:
                        sql_type = SqlType.int()
                    else:
                        sql_type = SqlType.big_int()

            elif pd.api.types.is_float_dtype(dtype):
                sql_type = SqlType.double()

            elif pd.api.types.is_bool_dtype(dtype):
                sql_type = SqlType.bool()

            elif pd.api.types.is_string_dtype(dtype):
                # Convert NaN to None (NULL in Hyper file)
                df[col_name] = df[col_name].replace({pd.NA: None, pd.NaT: None, float("nan"): None})
                max_length = df[col_name].str.len().max()
                if max_length <= 255:
                    sql_type = SqlType.char(int(max_length))
                else:
                    sql_type = SqlType.text()

            elif pd.api.types.is_datetime64_any_dtype(dtype):
                # Convert datetime to date format 'YYYY-MM-DD'
                df[col_name] = df[col_name].dt.strftime("%Y-%m-%d")
                df[col_name] = df[col_name].replace({pd.NA: None, pd.NaT: None, float("nan"): None})
                sql_type = SqlType.text()  # Store as text since it's no longer a datetime

            else:
                sql_type = SqlType.text()

            columns[col_name] = sql_type

        return columns

    def add_dataframes_to_hyper(self, dataframes: list[pd.DataFrame], table_names: list[str], hyper_file_path: str, drop_null_columns: bool = False) -> None:
        """
        Adds multiple DataFrames to a single Tableau Hyper file with specified column data types.

        Args:
            dataframes: List of DataFrames to add to the Hyper file.
            table_names: List of table names corresponding to the DataFrames.
            hyper_file_path: Path where the Hyper file should be saved.
            drop_null_columns: If True, drops columns with all null values before adding to the Hyper file.
        """
        try:
            with HyperProcess(telemetry=self.telemetry) as hyper:
                with Connection(
                    endpoint=hyper.endpoint, database=hyper_file_path, create_mode=CreateMode.CREATE_AND_REPLACE
                ) as connection:
                    # Create the "Extract" schema
                    connection.catalog.create_schema("Extract")

                    for df, table_name in zip(dataframes, table_names):
                        if drop_null_columns:
                            df = df.dropna(axis=1, how="all")

                        print(
                            f"Adding DataFrame to Hyper file as table {table_name} with {df.shape[0]} rows and {df.shape[1]} columns..."
                        )
                        col_types = self.pandas_to_smallest_sqltype(df)

                        # Define the table schema
                        table = TableDefinition(table_name)
                        for column, sql_type in col_types.items():
                            table.add_column(column, sql_type)

                        # Create the table in the Hyper file
                        connection.catalog.create_table(table)

                        # Insert the DataFrame into the table
                        with Inserter(connection, table) as inserter:
                            inserter.add_rows(df.values)
                            inserter.execute()
                    print(f"DataFrames have been added to {hyper_file_path}.")
        except Exception as e:
            print(e)
            raise

def duckdb_table_to_hyper(
    duckdb_connection: duckdb.DuckDBPyConnection, 
    table_name: str, 
    hyper_connection: Connection
) -> None:
    """
    Transfer a DuckDB table to a Hyper file, casting any datetime or date columns as strings.

    Args:
        duckdb_connection (duckdb.DuckDBPyConnection): The active DuckDB connection.
        table_name (str): The name of the table in DuckDB to transfer.
        hyper_connection (TableauHyperAPI.Connection): The active Hyper connection to write to.

    Behavior:
        This function reads the schema of the table in DuckDB, modifies any `TIMESTAMP`
        or `DATE` columns by casting them as `VARCHAR` (to handle dates as strings),
        then queries the modified table and transfers it to a Hyper file.
        The data types of each column are mapped from DuckDB to Hyper SQL types.

    Example:
        duckdb_conn = duckdb.connect('my_duckdb_file.duckdb')
        hyper_conn = Connection(hyper_endpoint, 'output_file.hyper')
        duckdb_table_to_hyper(duckdb_conn, 'my_table', hyper_conn)

    Returns:
        None
    """
    # Get table schema

    table_schema = duckdb_connection.execute(f"DESCRIBE {table_name}").fetchdf()
    select_columns = []

    # Loop through each column and cast date/time columns as VARCHAR
    for column_name, column_type in zip(table_schema['column_name'], table_schema['column_type']):
        if 'TIMESTAMP' in column_type.upper() or 'DATE' in column_type.upper():
            select_columns.append(f"CAST({column_name} AS VARCHAR) AS {column_name}")
        else:
            select_columns.append(column_name)

    # Build and execute query to cast columns
    query = f"SELECT {', '.join(select_columns)} FROM {table_name}"
    df = duckdb_connection.execute(query).df()

    # Define column types for the Hyper file
    columns = []
    for column_name, dtype in zip(df.columns, df.dtypes):
        if 'int' in str(dtype):
            columns.append((column_name, SqlType.big_int()))
        elif 'float' in str(dtype):
            columns.append((column_name, SqlType.double()))
        elif 'bool' in str(dtype):
            columns.append((column_name, SqlType.bool()))
        else:
            columns.append((column_name, SqlType.text()))  # Dates and strings as text

    # Create table definition in Hyper
    table_definition = TableDefinition(table_name)
    for column_name, sql_type in columns:
        table_definition.add_column(column_name, sql_type)
    hyper_connection.catalog.create_table(table_definition)

    # Insert data into the Hyper file
    with Inserter(hyper_connection, table_definition) as inserter:
        rows = df.where(pd.notnull(df), None).itertuples(index=False, name=None)
        inserter.add_rows(rows)
        inserter.execute()
    return None

def save_duckdb_tables_to_hyper(
    duckdb_connection: duckdb.DuckDBPyConnection, 
    table_names: List[str], 
    hyper_file_path: str
) -> None:
    """
    Save multiple DuckDB tables into a single Hyper file.
    Args:
        duckdb_connection (duckdb.DuckDBPyConnection): The active DuckDB connection.
        table_names (list of str): A list of DuckDB table names to transfer to Hyper.
        hyper_file_path (str): The file path for the Hyper file to be created.
    Behavior:
        This function loops over multiple DuckDB table names, converts each of them
        into a Hyper table by calling the 'duckdb_table_to_hyper' function, and
        writes them into a single Hyper file specified by the 'hyper_file_path'.
    Example:
        duckdb_conn = duckdb.connect('my_duckdb_file.duckdb')
        save_duckdb_tables_to_hyper(duckdb_conn, ['table1', 'table2'], 'output_file.hyper')
    Returns:
        None
    """
    # Open Hyper process and connection, replacing the file if it already exists
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database=hyper_file_path,
                        create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
            # Iterate over each table and save it to Hyper
            for table_name in table_names:
                duckdb_table_to_hyper(duckdb_connection, table_name, connection)
    return None



