import os
import duckdb
import pandas as pd
import polars as pl


class SalesforceDataSaver:
    def __init__(self, default_output_path: str = None):
        """
        Initializes the DataSaver with a default output path.

        Args:
            default_output_path (str, optional): The default path to save files. If not provided, it will create an 'output' folder in the root of the project.
        """
        self.default_output_path = default_output_path or os.path.join(os.getcwd(), "output")

    def save_data(self, df, table_name: str, output_format: str, output_path: str = None):
        """
        Saves the DataFrame to the specified format and path.

        Args:
            df (pd.DataFrame or pl.DataFrame): The DataFrame to save.
            table_name (str): The name of the table/file.
            output_format (str): The format to save the data in ('csv', 'parquet', 'duckdb', 'polars').
            output_path (str, optional): The path to save the file. If not provided, it will use the default output path.

        Raises:
            ValueError: If the output format is unsupported.
        """
        output_path = output_path or self.default_output_path

        if not os.path.exists(output_path):
            os.makedirs(output_path)

        try:
            match output_format:
                case "csv":
                    if isinstance(df, pd.DataFrame):
                        df.to_csv(f"{output_path}/{table_name}.csv", index=False, encoding="utf-8")
                    elif isinstance(df, pl.DataFrame):
                        df.write_csv(f"{output_path}/{table_name}.csv")
                case "parquet":
                    if isinstance(df, pd.DataFrame):
                        df.to_parquet(f"{output_path}/{table_name}.parquet", index=False)
                    elif isinstance(df, pl.DataFrame):
                        df.write_parquet(f"{output_path}/{table_name}.parquet")
                case "duckdb":
                    if isinstance(df, pd.DataFrame):
                        con = duckdb.connect(f"{output_path}/{table_name}.duckdb")
                        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
                        con.close()
                    elif isinstance(df, pl.DataFrame):
                        con = duckdb.connect(f"{output_path}/{table_name}.duckdb")
                        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
                        con.close()
                case "polars":
                    if isinstance(df, pl.DataFrame):
                        return df
                case "dataframe":
                    if isinstance(df, pd.DataFrame):
                        return df
                case _:
                    raise ValueError(f"Unsupported output format: {output_format}")
        except Exception as e:
            raise RuntimeError(f"Failed to save data for {table_name}: {e}")
