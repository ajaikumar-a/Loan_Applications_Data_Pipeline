import os
import json
from pyspark.sql import SparkSession

def create_spark_session():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(root, "configs", "spark_config.json")

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_path, "r") as config_file:
        config = json.load(config_file)

    spark_builder = (SparkSession
                     .builder
                     .appName([config["spark.app.name"]])
                     .config("spark.hadoop.io.nativeio.NativeIO.disable", "true"))

    for key, value in config.items():
        spark_builder = spark_builder.config(key, value)

    spark = spark_builder.getOrCreate()
    print(spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion())
    return spark

create_spark_session()
