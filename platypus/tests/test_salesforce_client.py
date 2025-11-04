import pytest
from platypus.salesforce_client.client import SalesforceClient
from simple_salesforce import Salesforce

def test_salesforce_client_initialization(monkeypatch):
    def mock_load_dotenv():
        pass

    def mock_getenv(key):
        return "mock_value"

    monkeypatch.setattr("platypus.salesforce_client.client.load_dotenv", mock_load_dotenv)
    monkeypatch.setattr("os.getenv", mock_getenv)

    client = SalesforceClient()
    assert isinstance(client.get_client(), Salesforce)