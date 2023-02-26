import pyspark
from pyspark.sql import SparkSession
import logging
from utils import get_dimension_lookup, get_struct_types
from file_processor import FileParser

def create_spark_session():
    """
    Creates a Spark session using pre-defined configuration.

    Returns:
        SparkSession: The created Spark session.
    """
    logging.warning(f"Trying to get configuration.")
    spark = (
        SparkSession.builder.appName("LiquorCapstone")
        .config("spark.executor.memory", "8g")
        .config("spark.executor.cores", "4")
        .config("spark.default.parallelism", "200")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.cores", "2")
        .getOrCreate()
    )
    return spark

def process_data(
    ss: pyspark.sql.session.SparkSession, s3_bucket: str, s3_key: str, output: str
    ) -> None:
    """
       Processes the dimension data and writes the results to S3.

       Args:
           ss (SparkSession): The Spark session.
           s3_bucket (str): The S3 bucket name.
           s3_key (str): The S3 key name.
           output (str): The output file name.

       Returns:
           None: The function does not return anything.
    """
    data_location = f"s3a://{s3_bucket}/Iowa_Liquor_Sales.csv"
    logging.warning(f"Data location is {data_location}.")
    logging.warning(f"Trying to get dimension lookups")

    schema = get_struct_types()
    df = ss.read.option("header", True).option("multiline","true").csv(data_location,schema=schema)

    dimension_lookup = get_dimension_lookup()
    fp = FileParser(dimension_lookup, df, s3_bucket, s3_key)
    fp.parse()

if __name__ == "__main__":
    session = create_spark_session()
    process_data(session, "sparkcapstonebucket", "test_key", "time_table.parquet")