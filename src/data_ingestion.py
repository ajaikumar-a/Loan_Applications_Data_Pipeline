import os.path
from src.spark_session import create_spark_session
from logs.logger import get_logger

logger = get_logger("data_ingestion.log")

def load_data(file_path):
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    file_path = os.path.join(root, file_path)

    spark = create_spark_session()
    logger.info(f"Loading data from {file_path}")

    df = (spark.read
          .option("header", True)
          .option("inferSchema", True)
          .format("csv")
          .load(file_path))
    logger.info(f"Loaded {df.count()} records.")

    return df

if __name__ == '__main__':
    df = load_data("data/raw/application_data.csv")
    df.show()