import pytest
from platypus.salesforce_client.data_fetcher import SalesforceDataFetcher
from simple_salesforce import Salesforce
import pyarrow as pa

@pytest.fixture
def mock_salesforce():
    class MockSalesforce:
        def query_all(self, query):
            return {"records": [{"Id": "001", "Name": "Test"}]}

        class bulk:
            @staticmethod
            def __getattr__(name):
                return MockSalesforce()

            def query(self, query):
                return [{"Id": "001", "Name": "Test"}]

    return MockSalesforce()

def test_fetch_data(mock_salesforce):
    fetcher = SalesforceDataFetcher(mock_salesforce)
    table = fetcher.fetch_data("SELECT Id, Name FROM Account", "Account")
    assert isinstance(table, pa.Table)
    assert table.num_rows == 1
    assert table.num_columns == 2