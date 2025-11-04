import os

import duckdb
import pandas as pd
import polars as pl


class DataSaver:
    def __init__(self, default_output_path: str = None):
        """
        Initializes the DataSaver with a default output path.
        """
        self.default_output_path = default_output_path or os.path.join(os.getcwd(), "output")

    def save_data(self, df, table_name: str, output_format: str, output_path: str = None):
        """
        Saves the DataFrame to the specified format and path.
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
                        df.to_parquet(f"{output_path}/{table_name}.parquet", index=False, engine="pyarrow")
                    elif isinstance(df, pl.DataFrame):
                        df.write_parquet(f"{output_path}/{table_name}.parquet", use_pyarrow=True)
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
                    else:
                        raise ValueError("DataFrame must be a Polars DataFrame for 'polars' output format")
                case _:
                    raise ValueError(f"Unsupported output format: {output_format}")
        except Exception as e:
            raise RuntimeError(f"Failed to save data for {table_name}: {e}")
