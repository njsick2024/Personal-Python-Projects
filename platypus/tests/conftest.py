import pytest
from platypus.salesforce_client.data_saver import SalesforceDataSaver
from platypus.dremio_client.data_saver import DataSaver

@pytest.fixture
def salesforce_data_saver():
    return SalesforceDataSaver()

@pytest.fixture
def dremio_data_saver():
    return DataSaver()