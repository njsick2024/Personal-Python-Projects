import os
from typing import Dict

import duckdb
import pandas as pd


def read_queries_from_file(file_path: str) -> list[str]:
    """
    Reads SQL queries from a file, splitting them by semicolon.

    Args:
        file_path (str): The path to the file containing SQL queries.

    Returns:
        list[str]: A list of SQL queries.
    """
    with open(file_path, "r") as file:
        queries = file.read().split(";")
        queries = [query.strip() for query in queries if query.strip()]
    return queries


def convert_file(input_path: str, output_path: str, read_func, write_func, input_ext: str, output_ext: str) -> None:
    """
    General function to convert files from one format to another.

    Args:
        input_path (str): The path to the input file or folder containing input files.
        output_path (str): The path where the output file(s) should be saved.
        read_func (callable): Function to read the input file.
        write_func (callable): Function to write the output file.
        input_ext (str): Extension of the input files.
        output_ext (str): Extension of the output files.
    """
    try:
        if os.path.isfile(input_path):
            # Single file case
            if not input_path.endswith(input_ext):
                print(f"Skipping non-{input_ext} file: {input_path}")
                return

            # Determine output path
            if output_path is None:
                output_file_path = os.path.splitext(input_path)[0] + output_ext
            else:
                output_file_name = os.path.splitext(os.path.basename(input_path))[0] + output_ext
                output_file_path = os.path.join(output_path, output_file_name)

            # Convert the single file
            print(f"Reading {input_ext.upper()}", input_path)
            df = read_func(input_path, low_memory=False)
            write_func(df, output_file_path)
            print(f"Converted '{input_path}' to '{output_file_path}'.")

        elif os.path.isdir(input_path):
            # Folder case
            if output_path is None:
                output_path = input_path  # Save output files in the same folder as the input files

            # Ensure the output folder exists
            os.makedirs(output_path, exist_ok=True)

            # Loop through all input files in the folder
            for filename in os.listdir(input_path):
                if filename.endswith(input_ext):
                    input_file_path = os.path.join(input_path, filename)
                    output_file_name = os.path.splitext(filename)[0] + output_ext
                    output_file_path = os.path.join(output_path, output_file_name)

                    # Convert the input file
                    df = read_func(input_file_path)
                    write_func(df, output_file_path)
                    print(f"Converted '{input_file_path}' to '{output_file_path}'.")

        else:
            print(f"'{input_path}' is neither a valid file nor a directory.")
    except Exception as e:
        print(f"Failed to convert {input_ext.upper()} to {output_ext.upper()}: {e}")


def convert_csv_to_parquet(input_path: str, output_path: str = None) -> None:
    """
    Converts CSV files to Parquet. If a folder is provided, it converts all CSV files in the folder.
    Exapmle:

        import platypus

        csv = r"locations.csv"
        platypus.convert_csv_to_parquet(csv, output_path="output")

    Args:
        input_path (str): The path to the input CSV file or folder containing CSV files.
        output_path (str): The path where the output Parquet file(s) should be saved.
        If not provided, Parquet files are saved in the same location as the CSV files.
        You do not need to specify the output file_name or add .parquet.
        Define an output location one if you need to send it to a specific location other than the default
    """
    convert_file(input_path, output_path, pd.read_csv, pd.DataFrame.to_parquet, ".csv", ".parquet")


def convert_csv_to_duckdb(input_path: str, output_path: str = None) -> None:
    """
    Converts CSV files to DuckDB. If a folder is provided, it converts all CSV files in the folder.

    Args:
        input_path (str): The path to the input CSV file or folder containing CSV files.
        output_path (str): The path where the output DuckDB file(s) should be saved.
        If not provided, DuckDB files are saved in the same location as the CSV files.
    """

    def write_func(df, output_file_path):
        con = duckdb.connect(output_file_path)
        con.execute(f"CREATE TABLE {os.path.splitext(os.path.basename(output_file_path))[0]} AS SELECT * FROM df")
        con.close()

    convert_file(input_path, output_path, pd.read_csv, write_func, ".csv", ".duckdb")


def convert_parquet_to_duckdb(input_path: str, output_path: str = None) -> None:
    """
    Converts Parquet files to DuckDB. If a folder is provided, it converts all Parquet files in the folder.

    Args:
        input_path (str): The path to the input Parquet file or folder containing Parquet files.
        output_path (str): The path where the output DuckDB file(s) should be saved.
        If not provided, DuckDB files are saved in the same location as the Parquet files.
    """

    def write_func(df, output_file_path):
        con = duckdb.connect(output_file_path)
        con.execute(f"CREATE TABLE {os.path.splitext(os.path.basename(output_file_path))[0]} AS SELECT * FROM df")
        con.close()

    convert_file(input_path, output_path, pd.read_parquet, write_func, ".parquet", ".duckdb")


def convert_duckdb_to_parquet(input_path: str, output_path: str = None) -> None:
    """
    Converts DuckDB files to Parquet. If a folder is provided, it converts all DuckDB files in the folder.

    Args:
        input_path (str): The path to the input DuckDB file or folder containing DuckDB files.
        output_path (str): The path where the output Parquet file(s) should be saved.
        If not provided, Parquet files are saved in the same location as the DuckDB files.
    """

    def read_func(input_file_path):
        con = duckdb.connect(input_file_path)
        df = con.execute("SELECT * FROM df").fetchdf()
        con.close()
        return df

    convert_file(input_path, output_path, read_func, pd.DataFrame.to_parquet, ".duckdb", ".parquet")


def merge_csv_files(output_filename: str, folder_path: str = None, file_paths: list[str] = None) -> None:
    """
    Merges multiple CSV files into a single CSV file.

    Args:
        output_filename (str): The name of the output CSV file.
        folder_path (str, optional): The path to the folder containing CSV files to merge.
        file_paths (list[str], optional): A list of file paths to CSV files to merge.
    """
    if folder_path:
        file_paths = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith(".csv")]

    if not file_paths:
        raise ValueError("No CSV files provided for merging.")

    # Read the first file to get the schema
    first_df = pd.read_csv(file_paths[0])
    first_columns = first_df.columns
    first_dtypes = first_df.dtypes

    # Check that all files have the same schema
    for file in file_paths[1:]:
        df = pd.read_csv(file)
        if not df.columns.equals(first_columns):
            raise ValueError(f"File {file} has different columns.")
        if not df.dtypes.equals(first_dtypes):
            raise ValueError(f"File {file} has different data types.")

    combined_df = pd.concat([pd.read_csv(file) for file in file_paths])
    combined_df.to_csv(output_filename, index=False)
    print(f"Merged CSV files into {output_filename}")


def merge_parquet_files(output_filename: str, folder_path: str = None, file_paths: list[str] = None) -> None:
    """
    Merges multiple Parquet files into a single Parquet file.

    Args:
        output_filename (str): The name of the output Parquet file.
        folder_path (str, optional): The path to the folder containing Parquet files to merge.
        file_paths (list[str], optional): A list of file paths to Parquet files to merge.
    """
    if folder_path:
        file_paths = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith(".parquet")]

    if not file_paths:
        raise ValueError("No Parquet files provided for merging.")

    # Read the first file to get the schema
    first_df = pd.read_parquet(file_paths[0])
    first_columns = first_df.columns
    first_dtypes = first_df.dtypes

    # Check that all files have the same schema
    for file in file_paths[1:]:
        df = pd.read_parquet(file)
        if not df.columns.equals(first_columns):
            raise ValueError(f"File {file} has different columns.")
        if not df.dtypes.equals(first_dtypes):
            raise ValueError(f"File {file} has different data types.")

    combined_df = pd.concat([pd.read_parquet(file) for file in file_paths])
    combined_df.to_parquet(output_filename, index=False)
    print(f"Merged Parquet files into {output_filename}")


def load_parquet_files_to_duckdb(duckdb_connection, files_to_load, files_dict):
    """
    Load multiple Parquet files into DuckDB tables using the provided connection.
    Args:
        conn (duckdb.DuckDBPyConnection): The active connection to DuckDB.
        files_to_load (list): A list of filenames (without paths) to be loaded into DuckDB.
        files_dict (dict): A dictionary where the keys are filenames (without paths) and the values are the full paths to the Parquet files.
    Behavior:
        For each file in 'files_to_load', this function retrieves its corresponding
        file path from 'files_dict' and creates a new table in DuckDB (or replaces it
        if it already exists) with the same name as the file. The table is loaded with
        data from the corresponding Parquet file using DuckDB's 'parquet_scan' feature.
    Example:
        conn = duckdb.connect('my_database.duckdb')
        files_to_load = ['file1', 'file2']
        files_dict = {'file1': '/path/to/file1.parquet', 'file2': '/path/to/file2.parquet'}
        load_parquet_files_to_duckdb(conn, files_to_load, files_dict)
    Returns:
        None
    """
    for file in files_to_load:
        # Fetch the corresponding path from the dictionary
        file_path = files_dict.get(file)
        if file_path:
            # Load the parquet file into DuckDB under a table with the same name as the key
            duckdb_connection.execute(f"CREATE OR REPLACE TABLE {file} AS SELECT * FROM parquet_scan('{file_path}')")
    return None


def get_duckdb_tables_info(duckdb_connection: duckdb.DuckDBPyConnection) -> None:
    """
    Retrieve information about tables in a DuckDB connection, including row and column counts.

    Args:
        duckdb_connection (duckdb.DuckDBPyConnection): The active DuckDB connection to query.
    Behavior:
        This function fetches the list of tables from the connected DuckDB database.
        For each table, it retrieves the number of rows and columns. The result is
        printed as a dataframe with columns for the table names, row counts, and column counts.
    Example:
        duckdb_conn = duckdb.connect('my_database.duckdb')
        get_duckdb_tables_info(duckdb_conn)
    Returns:
        None

    """
    # Get table names

    tables_df = duckdb_connection.execute("SHOW TABLES").fetchdf()
    num_rows = []
    num_cols = []
    # Fetch the row and column counts for each table
    for table in tables_df["name"]:
        row_count = duckdb_connection.execute(f"SELECT COUNT(*) AS num_rows FROM {table}").fetchone()[0]
        num_rows.append(row_count)
        col_count = duckdb_connection.execute(f"PRAGMA table_info({table})").fetchdf().shape[0]
        num_cols.append(col_count)
    # Add row and column counts to the dataframe
    tables_df["num_rows"] = num_rows
    tables_df["num_cols"] = num_cols
    # Print the table info
    print(tables_df)
    return None


def get_file_names_from_folder(folder_path: str) -> Dict[str, str]:
    """
    Get a dictionary of file names (without extensions) and their full paths from a folder.
    Args:
        folder_path (str): The path to the folder containing the files.
    Behavior:
        This function scans the specified folder, retrieves all file names, strips the
        file extensions, and stores them as keys in a dictionary. The corresponding values
        are the full paths to the files.
    Example:
        file_dict = get_file_names('/path/to/folder')
        # Output example: {'file1': '/path/to/folder/file1.ext', 'file2': '/path/to/folder/file2.ext'}
    Returns:
        dict: A dictionary where keys are file names without extensions, and values are full paths.
    """
    files = {}
    for filename in os.listdir(folder_path):
        name = filename.split(".")[0]
        files[name] = os.path.join(folder_path, filename)
    return files
    return files
