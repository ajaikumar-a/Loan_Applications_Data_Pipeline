from src.data_ingestion import load_data, logger
from pyspark.sql import functions as f
from logs.logger import get_logger

def clean_data():
    # Load data
    df = load_data("data/raw/application_data.csv")

    logger.info("Initializing data cleaning")

    # Total no.of dataframe rows
    print("Total no.of rows: ", df.count())

    # Dataframe schema info
    df.printSchema()

    # Checking the count of duplicate records
    df.groupby(df.columns)\
        .count()\
        .where(f.col('count') > 1)\
        .select(f.sum('count'))\
        .show() # No duplicate records found

    # Removing duplicate records
    df = df.dropDuplicates(["SK_ID_CURR"])
    logger.info("Removed duplicate records")

    # Round numeric values to two decimals
    df = (df.withColumn("AMT_INCOME_TOTAL", f.round("AMT_INCOME_TOTAL", 2))
          .withColumn("AMT_CREDIT", f.round("AMT_CREDIT", 2))
          .withColumn("AMT_ANNUITY", f.round("AMT_ANNUITY", 2))
          .withColumn("AMT_GOODS_PRICE", f.round("AMT_GOODS_PRICE", 2))
          .withColumn("REGION_POPULATION_RELATIVE", f.round("REGION_POPULATION_RELATIVE", 2))
          .withColumn("APARTMENTS_AVG", f.round("APARTMENTS_AVG", 2))
          .withColumn("BASEMENTAREA_AVG", f.round("BASEMENTAREA_AVG", 2))
          .withColumn("YEARS_BEGINEXPLUATATION_AVG", f.round("YEARS_BEGINEXPLUATATION_AVG", 2))
          .withColumn("YEARS_BUILD_AVG", f.round("YEARS_BUILD_AVG", 2))
          .withColumn("COMMONAREA_AVG", f.round("COMMONAREA_AVG", 2))
          .withColumn("ELEVATORS_AVG", f.round("ELEVATORS_AVG", 2)))
    logger.info("Rounded numeric values to two decimals")

    # Drop unnecessary columns
    df_dropped_columns = df.drop("TARGET", "EXT_SOURCE_1", "EXT_SOURCE_2", "EXT_SOURCE_3", "DAYS_BIRTH", "DAYS_EMPLOYED", "DAYS_REGISTRATION", "DAYS_ID_PUBLISH", "DAYS_LAST_PHONE_CHANGE")
    logger.info("Dropped unnecessary columns")

    return df_dropped_columns

if __name__ == "__main__":
    df_cleaned = clean_data()
    df_cleaned.show(10)