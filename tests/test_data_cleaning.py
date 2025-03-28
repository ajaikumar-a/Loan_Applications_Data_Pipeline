import pytest
from src.data_cleaning import clean_data

def test_data_cleaning():
    df = clean_data()
    columns = ["TARGET", "EXT_SOURCE_1", "EXT_SOURCE_2", "EXT_SOURCE_3", "DAYS_BIRTH", "DAYS_EMPLOYED", "DAYS_REGISTRATION", "DAYS_ID_PUBLISH", "DAYS_LAST_PHONE_CHANGE"]
    for col in columns:
        assert col not in df.columns