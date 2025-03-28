import pytest
from src.data_transformation import transform_data

def test_data_transformation():
    df = transform_data()

    columns = ["DEBT_TO_INCOME_RATIO", "LTV", "INCOME_CATEGORY"]
    for col in columns:
        assert col in df.columns, f"{col} exists in dataframe"
    
    