import pytest
from platypus.salesforce_client.query_executor import SalesforceQueryExecutor
from platypus.salesforce_client.data_fetcher import SalesforceDataFetcher
from platypus.salesforce_client.data_saver import SalesforceDataSaver
import pandas as pd

@pytest.fixture
def mock_query_executor(monkeypatch):
    class MockSalesforceClient:
        def get_client(self):
            return "mock_client"

    class MockSalesforceDataFetcher:
        def __init__(self, client):
            pass

        def fetch_and_clean_data(self, query, object_nm, full_output_path=None, output_format='dataframe', bulk=False, limit=None):
            return pd.DataFrame({"Id": [1], "Name": ["Test"]})

    class MockSalesforceDataSaver:
        def save_data(self, df, table_name, output_format, output_path=None):
            pass

    monkeypatch.setattr("platypus.salesforce_client.client.SalesforceClient", MockSalesforceClient)
    monkeypatch.setattr("platypus.salesforce_client.data_fetcher.SalesforceDataFetcher", MockSalesforceDataFetcher)
    monkeypatch.setattr("platypus.salesforce_client.data_saver.SalesforceDataSaver", MockSalesforceDataSaver)

    return SalesforceQueryExecutor()

def test_execute_and_save(mock_query_executor):
    df = mock_query_executor.execute_and_save("SELECT Id, Name FROM Account", "test_table", "dataframe")
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (1, 2)