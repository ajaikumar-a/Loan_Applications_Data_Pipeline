from src.data_cleaning import clean_data, logger
from pyspark.sql import functions as f
import sys
import os

def transform_data():
    # Clean data
    df_cleaned = clean_data()
    logger.info("Finished data cleaning")

    # Calculate debt-to-income ratio
    df_cleaned = df_cleaned.withColumn(
        "DEBT_TO_INCOME_RATIO",
        f.round(f.col("AMT_CREDIT") / f.col("AMT_INCOME_TOTAL"), 2)
    )
    logger.info("Calculated debt-to-income ratio")

    # Calculate loan-to-value (LTV)
    df_cleaned = df_cleaned.withColumn(
        "LTV",
        f.round(f.col("AMT_CREDIT") / f.col("AMT_GOODS_PRICE"), 2)
    )
    logger.info("Calculated LTV")

    # Categorize income
    df_cleaned = df_cleaned.withColumn(
        "INCOME_CATEGORY",
        f.when(f.col("AMT_INCOME_TOTAL") < 100000, "Low")
         .when(f.col("AMT_INCOME_TOTAL") < 600000, "Medium")
         .otherwise("High")
    )
    logger.info("Created income categories")

    # Create summary dataframes
    df_summary_contract_type = df_cleaned.groupBy("NAME_CONTRACT_TYPE").agg(
        f.count("SK_ID_CURR").alias("TOTAL_NO_OF_LOANS"),
        f.sum("AMT_CREDIT").alias("TOTAL_LOAN_AMOUNT")
    )
    df_summary_contract_type.show()

    df_summary_occupation_type = df_cleaned.groupBy("OCCUPATION_TYPE").agg(
        f.count("SK_ID_CURR").alias("TOTAL_NO_OF_LOANS"),
        f.sum("AMT_CREDIT").alias("TOTAL_LOAN_AMOUNT")
    )
    df_summary_occupation_type.show()

    df_summary_income_category = df_cleaned.groupBy("INCOME_CATEGORY").agg(
        f.count("SK_ID_CURR").alias("TOTAL_NO_OF_LOANS"),
        f.sum("AMT_CREDIT").alias("TOTAL_LOAN_AMOUNT")
    )
    df_summary_income_category.show()
    logger.info("Created summary dataframes")

    # Save the processed data
    (df_cleaned.write
     .mode("overwrite")
     .format("parquet")
     .option("header", True)
     .save("data/processed/cleaned_data/"))

if __name__ == "__main__":
    transform_data()


