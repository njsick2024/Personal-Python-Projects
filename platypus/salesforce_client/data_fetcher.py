import pyarrow as pa
import polars as pl
from simple_salesforce import Salesforce
from platypus.utils.data_cleaning import clean_dataframe, clean_column_names
import pandas as pd


class SalesforceDataFetcher:
    """
    A class to fetch data from Salesforce and return it as an Arrow Table.

    Attributes:
        sf (Salesforce): An instance of the Salesforce client.
    """

    def __init__(self, sf: Salesforce):
        self.sf = sf

    def fetch_data(self, query: str, object_nm: str, bulk: bool = False, limit: int = None) -> pa.Table:
        """
        Fetches data from Salesforce and returns it as an Arrow Table.

        Args:
            query (str): The SOQL query to execute.
            object_nm (str): The name of the Salesforce object to query.
            bulk (bool, optional): Whether to use bulk query. Defaults to False.
            limit (int, optional): The limit on the number of records to fetch. Defaults to None.

        Returns:
            pa.Table: The fetched data as an Arrow Table.
        """
        if limit:
            query = query + f" LIMIT {limit}"
        if bulk:
            data = self.sf.bulk.__getattr__(object_nm).query(query)
        else:
            data = self.sf.query_all(query)["records"]

        # Convert data to Arrow Table and drop the 'attributes' column
        table = pa.Table.from_pydict({k: [d[k] for d in data] for k in data[0] if k != "attributes"})
        return table

    def fetch_and_clean_data(
        self,
        query: str,
        object_nm: str,
        full_output_path: str = None,
        output_format: str = "dataframe",
        bulk: bool = False,
        limit: int = None,
    ):
        """
        Fetches and cleans data from Salesforce.

        Args:
            query (str): The SOQL query to execute.
            object_nm (str): The name of the Salesforce object to query.
            full_output_path (str, optional): The full path to save the output file. Defaults to None.
            output_format (str, optional): The format to save the data ('csv', 'parquet', 'duckdb', 'polars', 'dataframe'). Defaults to 'dataframe'.
            bulk (bool, optional): Whether to use bulk query. Defaults to False.
            limit (int, optional): The limit on the number of records to fetch. Defaults to None.

        Returns:
            DataFrame: The cleaned data as a DataFrame.
        """

        print("\n", f"Executing {object_nm} Query")

        table = self.fetch_data(query, object_nm, bulk, limit)

        match output_format:
            case "polars":
                df = pl.from_arrow(table)
                df = clean_dataframe(df)
            case "dataframe":
                df = table.to_pandas()
                df = clean_dataframe(df)
            case "csv":
                df = pl.from_arrow(table)
                df = clean_column_names(df)
            case _:
                df = pl.from_arrow(table)
                df = clean_dataframe(df)

        return df
