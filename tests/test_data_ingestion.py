import pytest
from src.data_ingestion import load_data

def test_load_data():
    df = load_data("data/raw/application_data.csv")

    assert df is not None
    assert df.count() > 0