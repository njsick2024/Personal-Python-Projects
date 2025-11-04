import re
from datetime import datetime

import numpy as np
import pandas as pd
import polars as pl


def normalize_date_str(date_str: str) -> str:
    """
    Normalizes date strings to a consistent format (YYYY-MM-DD).

    Handles various date formats including:
    - DD/MM/YYYY
    - MM/DD/YYYY
    - YYYY/MM/DD
    - DD-MM-YYYY
    - MM-DD-YYYY
    - YYYY-MM-DD

    Args:
        date_str: Input date string

    Returns:
        Normalized date string in YYYY-MM-DD format

    Examples:
        >>> normalize_date_str("01/02/2023")
        '2023-01-02'
        >>> normalize_date_str("2023-01-02")
        '2023-01-02'
    """
    if not isinstance(date_str, str):
        return date_str

    # Remove any whitespace
    date_str = date_str.strip()

    # Replace all separators with '-'
    date_str = re.sub(r"[/\s]", "-", date_str)

    # Try parsing with different formats
    date_formats = ["%Y-%m-%d", "%d-%m-%Y", "%m-%d-%Y"]

    for fmt in date_formats:
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    return date_str


def convert_to_datetime(df: pd.DataFrame | pl.DataFrame) -> pd.DataFrame | pl.DataFrame:
    """
    Converts all columns with 'date' in the name to datetime, normalizing the format.

    This function applies the `normalize_date_str` function to each column containing 'date' in its name,
    converting the column to a datetime format.

    Args:
        df (pd.DataFrame or pl.DataFrame): The DataFrame to process.

    Returns:
        pd.DataFrame or pl.DataFrame: The DataFrame with date columns normalized.

    Example:
        >>> import pandas as pd
        >>> df = pd.DataFrame({'start_date': ['2021-01-01', '2021-02-01']})
        >>> convert_to_datetime(df)
    """
    if isinstance(df, pl.DataFrame):
        for col in df.columns:
            if "date" in col.lower():
                df = df.with_columns(pl.col(col).map_elements(normalize_date_str, return_dtype=pl.Date))
    elif isinstance(df, pd.DataFrame):
        for col in df.columns:
            if "date" in col.lower():
                df[col] = df[col].apply(normalize_date_str)
    return df


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans DataFrame column names by removing spaces and special characters.

    The function performs the following operations:
    1. Strips leading/trailing whitespace
    2. Converts to lowercase
    3. Replaces special characters with underscores

    Args:
        df: Input DataFrame, can be either pandas or polars DataFrame

    Returns:
        DataFrame with cleaned column names in the same format as input (pandas or polars)

    Examples:
        >>> df = pd.DataFrame({'Column 1': [1, 2], ' Column-2 ': [3, 4]})
        >>> cleaned_df = clean_column_names(df)
        >>> print(cleaned_df.columns)
        ['column_1', 'column_2']
    """
    if isinstance(df, pl.DataFrame):
        df.columns = [col.strip() for col in df.columns]
        df.columns = [re.sub(r"[^a-zA-Z0-9_]", "_", col.lower()) for col in df.columns]
    elif isinstance(df, pd.DataFrame):
        df.columns = [col.strip() for col in df.columns]
        df.columns = [re.sub(r"[^a-zA-Z0-9_]", "_", col.lower()) for col in df.columns]
    return df


def resolve_mixed_data_formats(df: pd.DataFrame | pl.DataFrame) -> pd.DataFrame | pl.DataFrame:
    """
    Converts columns with mixed data types to strings.

    This function identifies columns with mixed data types and converts them to strings to ensure consistency.

    Args:
        df (pd.DataFrame or pl.DataFrame): The DataFrame to process.

    Returns:
        pd.DataFrame or pl.DataFrame: The DataFrame with mixed data type columns converted to strings.

    Example:
        >>> import pandas as pd
        >>> df = pd.DataFrame({'mixed': [1, 'two', 3.0]})
        >>> resolve_mixed_data_formats(df)
    """
    if isinstance(df, pl.DataFrame):
        for col in df.columns:
            if df[col].dtype == pl.Object:
                df = df.with_columns(pl.col(col).cast(pl.Utf8))

    elif isinstance(df, pd.DataFrame):
        for col in df.columns:
            if df[col].apply(type).nunique() > 1:
                # df[col] = df[col].astype(str)
                df.loc[:, col] = df[col].astype(str)
    return df


def drop_all_null_columns(df: pd.DataFrame | pl.DataFrame) -> pd.DataFrame | pl.DataFrame:
    """
    Drops columns in a DataFrame that contain only null values.

    This function removes any columns from the DataFrame that are entirely null.

    Args:
        df (pd.DataFrame or pl.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame or pl.DataFrame: A DataFrame with columns containing only null values dropped.

    Example:
        >>> import pandas as pd
        >>> df = pd.DataFrame({'A': [None, None], 'B': [1, 2]})
        >>> drop_all_null_columns(df)
    """
    if isinstance(df, pd.DataFrame):
        return df.dropna(axis=1, how="all")
    elif isinstance(df, pl.DataFrame):
        return df.drop_nulls()
    else:
        raise TypeError("Input must be a pandas or polars DataFrame")


def replace_empty_with_null(df: pd.DataFrame | pl.DataFrame) -> pd.DataFrame | pl.DataFrame:
    """
    Replaces empty strings and string 'nan' (case-insensitive) with null (np.nan or None),
    for both Pandas and Polars DataFrames.
    Args:
        df (pd.DataFrame or pl.DataFrame): The input DataFrame.
    Returns:
        pd.DataFrame or pl.DataFrame: The cleaned DataFrame.
    """
    match df:
        case pd.DataFrame():
            for col in df.columns:
                if pd.api.types.is_string_dtype(df[col]):
                    df.loc[:, col] = df[col].map(
                        lambda x: np.nan if isinstance(x, str) and x.strip().lower() in {"", "nan"} else x
                    )

            return df
        case pl.DataFrame():
            lazy_df = df.lazy()
            expressions = []
            for col_name, dtype in zip(df.columns, df.dtypes):
                if dtype == pl.Utf8:
                    expr = (
                        pl.when(pl.col(col_name).str.strip_chars(" ").str.to_lowercase().is_in(["", "nan"]))
                        .then(pl.lit(value=None, dtype=pl.Utf8))
                        .otherwise(pl.col(col_name).str.strip_chars(" "))
                        .alias(col_name)
                    )
                else:
                    expr = pl.col(col_name)
                expressions.append(expr)
            return lazy_df.with_columns(expressions).collect()
        case _:
            raise TypeError("Input must be a Pandas or Polars DataFrame.")


def optimize_memory_usage(df: pd.DataFrame | pl.DataFrame) -> pd.DataFrame | pl.DataFrame:
    """
    Optimizes the memory usage of a DataFrame by converting data types to more efficient ones where possible.

    This function analyzes the data types of the columns in the input DataFrame and attempts to reduce memory usage by:
    - Converting string columns to categorical types if the cardinality is low.
    - Downcasting float64 columns to float32.
    - Downcasting int64 columns to int32 if the values fit within the int32 range.

    The function supports both Polars and Pandas DataFrames.

    Args:
        df (pd.DataFrame or pl.DataFrame): The DataFrame to optimize.
            - Supports both Pandas and Polars DataFrames.

    Returns:
        pd.DataFrame or pl.DataFrame: The optimized DataFrame with reduced memory usage.

    Prints:
        The function prints the original and new memory usage of the DataFrame, along with the absolute and percentage differences.

    Example:
        >>> import pandas as pd
        >>> df = pd.DataFrame({'A': [1, 2, 3], 'B': [4.0, 5.0, 6.0]})
        >>> optimized_df = optimize_memory_usage(df)
        >>> print(optimized_df)

        >>> import polars as pl
        >>> df = pl.DataFrame({'A': [1, 2, 3], 'B': [4.0, 5.0, 6.0]})
        >>> optimized_df = optimize_memory_usage(df)
        >>> print(optimized_df)
    """

    original_memory_usage = df.memory_usage(deep=True).sum() / (1024**2)

    if isinstance(df, pl.DataFrame):
        for col in df.columns:
            match df[col].dtype:
                case pl.Utf8:
                    unique_values = df[col].nunique()
                    # Cardinality threshold to 25%
                    if unique_values / len(df) < 0.25:
                        df = df.with_columns(pl.col(col).cast(pl.Categorical))
                case pl.Float64:
                    df = df.with_columns(pl.col(col).cast(pl.Float32))
                case pl.Int64:
                    if df[col].min() >= -2147483648 and df[col].max() <= 2147483647:
                        df = df.with_columns(pl.col(col).cast(pl.Int32))

    elif isinstance(df, pd.DataFrame):
        for col in df.columns:
            match df[col].dtype:
                case "float64":
                    df[col] = df[col].astype("float32")
                case "int64":
                    if df[col].min() >= -2147483648 and df[col].max() <= 2147483647:
                        df[col] = df[col].astype("int32")

    new_memory_usage = df.memory_usage(deep=True).sum() / (1024**2)
    absolute_difference = new_memory_usage - original_memory_usage
    percent_difference = (absolute_difference / original_memory_usage) * 100

    print(f"Original memory usage: {original_memory_usage:.2f} MB")
    print(f"New memory usage: {new_memory_usage:.2f} MB")
    print(f"Absolute difference: {absolute_difference:.2f} MB")
    print(f"Percent difference: {percent_difference:.2f}%")

    return df


def clean_polars_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    """
    Cleans a Polars DataFrame by cleaning column names and resolving mixed data types.

    Args:
        df (pl.DataFrame): The Polars DataFrame to clean.

    Returns:
        pl.DataFrame: The cleaned Polars DataFrame.
    """

    print("Cleaning Column Names")
    df = clean_column_names(df)

    print("Dropping Null Columns")
    df = df.select([col for col in df.columns if len(df[col].drop_nulls()) > 0])

    print("Nulling Empty Strings")
    df = replace_empty_with_null(df)

    print("Resolving Mixed Data Types")
    for col in df.columns:
        if df[col].dtype == pl.Object:
            df = df.with_columns(pl.col(col).cast(pl.Utf8))

    # print("Normalizing Date Columns") #!: TODO: make sure this works.!Most of the dates are strings so I need to create a regex to identify.
    # df = convert_to_datetime(dataframe)

    # print("Optimizing memory usage") #!: TODO: Need to do this on the batches.
    # df = optimize_memory_usage(df)

    return df


def clean_pandas_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans a Pandas DataFrame by cleaning column names and resolving mixed data types.

    Args:
        df (pd.DataFrame): The Pandas DataFrame to clean.

    Returns:
        pd.DataFrame: The cleaned Pandas DataFrame.
    """

    print("Cleaning Column Names")
    df = clean_column_names(df)

    print("Dropping Null Columns")
    df = drop_all_null_columns(df)

    print("Nulling Empty Strings")
    df = replace_empty_with_null(df)

    print("Resolving Mixed Data Types")
    df = resolve_mixed_data_formats(df)

    # print("Normalizing Date Columns")
    # df = convert_to_datetime(df)

    # print("Optimizing memory usage") #! TODO: Need to do this on the batches.
    # df = optimize_memory_usage(df)

    return df


def clean_dataframe(df: pd.DataFrame | pl.DataFrame) -> pd.DataFrame | pl.DataFrame:
    """
    Cleans a DataFrame by normalizing date columns, cleaning column names, replace empty string, nan values, and resolves mixed data formats.

    This function supports both Polars and Pandas DataFrames.

    Args:
        dataframe (pl.DataFrame or pd.DataFrame): The DataFrame to be cleaned.
            - If a Polars DataFrame is provided, it will be processed using Polars-specific cleaning functions.
            - If a Pandas DataFrame is provided, it will be processed using Pandas-specific cleaning functions.

    Returns:
        pl.DataFrame or pd.DataFrame: The cleaned DataFrame, with the same type as the input.

    Raises:
        TypeError: If the input is not a Polars or Pandas DataFrame.

    Example:
        >>> import pandas as pd
        >>> df = pd.DataFrame({'Date': ['2021-01-01', '2021-02-01'], 'Value': [10, 20]})
        >>> clean_df = clean_dataframe(df)
        >>> print(clean_df)

        >>> import polars as pl
        >>> df = pl.DataFrame({'Date': ['2021-01-01', '2021-02-01'], 'Value': [10, 20]})
        >>> clean_df = clean_dataframe(df)
        >>> print(clean_df)
    """
    match df:
        case pl.DataFrame():
            return clean_polars_dataframe(df)
        case pd.DataFrame():
            return clean_pandas_dataframe(df)
        case _:
            raise TypeError("Unsupported DataFrame type. Only Polars and Pandas DataFrames are supported.")
            raise TypeError("Unsupported DataFrame type. Only Polars and Pandas DataFrames are supported.")
