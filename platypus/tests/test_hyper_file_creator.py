import os
import pytest
import pandas as pd
from platypus.utils.hyper_file_creator import HyperFileCreator

@pytest.fixture
def hyper_file_creator():
    return HyperFileCreator()

def test_pandas_to_smallest_sqltype(hyper_file_creator):
    df = pd.DataFrame({"Id": [1], "Name": ["Test"]})
    col_types = hyper_file_creator.pandas_to_smallest_sqltype(df)
    assert col_types["Id"].type_name == "SMALLINT"
    assert col_types["Name"].type_name == "CHAR"

def test_add_dataframes_to_hyper(hyper_file_creator, tmp_path):
    df = pd.DataFrame({"Id": [1], "Name": ["Test"]})
    hyper_file_creator.add_dataframes_to_hyper([df], ["test_table"], tmp_path / "test.hyper")
    assert os.path.exists(tmp_path / "test.hyper")