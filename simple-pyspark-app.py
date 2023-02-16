import pyspark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    dayofweek,
    to_date,
    year,
    quarter,
    month,
    weekofyear,
)
from pyspark.sql.types import FloatType,StringType, IntegerType, StructField, StructType

import logging


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


def process_dim_data(
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

    schema = StructType([StructField("invoice_number", StringType(), True),
                     StructField("date", StringType(), False),
                     StructField("store_number", IntegerType(), False),
                     StructField("store_name", StringType(), False),
                     StructField("address", StringType(), False),
                     StructField("city", StringType(), False),
                     StructField("zip_code", StringType(), False),
                     StructField("store_location", StringType(), False),
                     StructField("county_number", IntegerType(), False),
                     StructField("county", StringType(), False),
                     StructField("category", IntegerType(), False),
                     StructField("category_name", StringType(), False),
                     StructField("vendor_number", IntegerType(), False),
                     StructField("vendor_name", StringType(), False),
                     StructField("item_number", IntegerType(), False),
                     StructField("item_description", StringType(), False),
                     StructField("pack", IntegerType(), False),
                     StructField("bottle_volume", StringType(), False),
                     StructField("state_bottle_cost", StringType(), False),
                     StructField("state_bottle_retail", StringType(), False),
                     StructField("bottles_sold", IntegerType(), False),
                     StructField("sale", StringType(), False),
                     StructField("volume_sold_liters", FloatType(), False),
                     StructField("volume_sold_gallons", FloatType(), False)])

    df = ss.read.option("header", True).option("multiline","true").csv(data_location,schema=schema)

    df = df.withColumn("date_ex", to_date("date", "MM/dd/yyyy"))

    dimension_lookup = {
        "Store": ["store_number", "store_name", "address", "store_location"],
        "Item": [
            "item_number",
            "item_description"
        ],
        "Vendor": ["vendor_number", "vendor_name"],
        "City": ["zip_code", "city"],
        "Category": ["category", "category_name"],
        "Item_Price": ["item_number","date","state_bottle_cost","state_bottle_retail"]
    }

    for key, val in dimension_lookup.items():
        part_key = key.lower() + "_table"+".parquet"
        logging.warning(f"Current dimension is {key}.")
        primary_key = val[0]
        logging.warning(f"primary key{primary_key}")
        inner_df = df.drop_duplicates([primary_key]).select(val)
        logging.warning(f"Length of dimension {key} is : {inner_df.count()}")
        logging.warning(f"Schema is : \n{inner_df.printSchema()}")
        inner_df.write.parquet(f"s3a://{s3_bucket}/{s3_key}/{part_key}").mode("overwrite")

    logging.warning(f"Trying to extract time dimension")

    # Extract time dimension
    time_dim = (
        df \
        .withColumn("weekend", dayofweek("date_ex").isin([1, 7]).cast("int"))
        .withColumn("year", year("date_ex"))
        .withColumn("month", month("date_ex"))
        .withColumn("quarter", quarter("date_ex"))
        .withColumn("weekofyear", weekofyear("date_ex"))
        .dropDuplicates(["date_ex", "weekend", "year", "month", "quarter"])
        .select(["date_ex", "weekend", "year", "month", "quarter"])
    )
    logging.warning(f"Length of time_dimension is {time_dim .count()}")
    logging.warning(f"Schema is : \n{time_dim.printSchema()}")
    time_dim.write.parquet(f"s3a://{s3_bucket}/{s3_key}/{output}").mode("overwrite")


if __name__ == "__main__":
    session = create_spark_session()
    process_dim_data(session, "sparkcapstonebucket", "test_key", "time_table.parquet")
