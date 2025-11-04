import pytest
import pandas as pd
import polars as pl
import os
from platypus.salesforce_client.data_saver import SalesforceDataSaver

@pytest.fixture
def data_saver():
    return SalesforceDataSaver()

def test_save_data_csv(data_saver, tmp_path):
    df = pd.DataFrame({"Id": [1], "Name": ["Test"]})
    data_saver.save_data(df, "test_table", "csv", tmp_path)
    assert os.path.exists(tmp_path / "test_table.csv")

def test_save_data_parquet(data_saver, tmp_path):
    df = pd.DataFrame({"Id": [1], "Name": ["Test"]})
    data_saver.save_data(df, "test_table", "parquet", tmp_path)
    assert os.path.exists(tmp_path / "test_table.parquet")

def test_save_data_duckdb(data_saver, tmp_path):
    df = pd.DataFrame({"Id": [1], "Name": ["Test"]})
    data_saver.save_data(df, "test_table", "duckdb", tmp_path)
    assert os.path.exists(tmp_path / "test_table.duckdb")
    