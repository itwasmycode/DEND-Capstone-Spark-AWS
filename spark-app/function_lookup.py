import logging
import pyspark

from pyspark.sql.functions import (
    col,
    dayofweek,
    year,
    quarter,
    month,
    weekofyear,
    regexp_replace,
)
from typing import List

def process_item_price(df: pyspark.sql.dataframe.DataFrame, s3_bucket: str, s3_key: str, column_list: List[str]) -> None:
    """
    Process item price and write to s3 bucket in parquet format.
    Args:
        df (pyspark.sql.dataframe.DataFrame):  PySpark dataframe containing item_price data.
        s3_bucket (str): The name of the S3 bucket to write the item_price dimension table to.
        s3_key (str): The S3 key to use when writing the time dimension table.
        process_list (List): List of column names.

    Returns:
        None
    """
    part_key = "item_price_table.parquet"
    logging.warning(f"Current dimension is item_price.")
    logging.warning(f"primary keys item_price, date")
    inner_df = df.drop_duplicates(column_list[:2]).select(column_list)
    inner_df = inner_df \
                .withColumn("state_bottle_retail",
                                regexp_replace(col('state_bottle_retail'), "[^0-9.]", "")) \
                    .withColumn("state_bottle_retail_dollar", col("state_bottle_retail").cast("double")) 
    inner_df = inner_df.withColumn("state_bottle_cost",
                                regexp_replace(col('state_bottle_cost'), "[^0-9.]", '')) \
                    .withColumn("state_bottle_cost_dollar", col("state_bottle_cost").cast("double")) \
                    .select(["item_number","date","state_bottle_cost_dollar","state_bottle_retail_dollar"])
    logging.warning(f"Length of dimension item_price is : {inner_df.count()}")
    logging.warning(f"Schema is : \n{inner_df.printSchema()}")
    inner_df.write.parquet(f"s3a://{s3_bucket}/{s3_key}/{part_key}",mode='overwrite')


def process_category(df: pyspark.sql.dataframe.DataFrame, s3_bucket: str, s3_key: str, column_list: List[str]) -> None:
    """
    Process category data and write to s3 bucket in parquet format.
    Args:
        df (pyspark.sql.dataframe.DataFrame):  PySpark dataframe containing category data.
        s3_bucket (str): The name of the S3 bucket to write the category dimension table to.
        s3_key (str): The S3 key to use when writing the time dimension table.
        process_list (List): List of column names.

    Returns: None
    """
    inner_df = df.withColumn("Category",col("Category").cast('int'))
    inner_df = df.drop_duplicates(column_list).select(column_list)
    logging.warning(f"Length of dimension category is : {inner_df.count()}")
    logging.warning(f"Schema is : \n{inner_df.printSchema()}")
    inner_df.write.parquet(f"s3a://{s3_bucket}/{s3_key}/category_table.parquet",mode='overwrite')

def process_date(df: pyspark.sql.dataframe.DataFrame, s3_bucket: str, s3_key: str, column_list: List[str]) -> None:
    """
    Processes a date DataFrame to create a time dimension table and writes it to S3.

    Args:
        df (DataFrame): PySpark dataframe containing date data.
        s3_bucket (str): The name of the S3 bucket to write the date dimension table to.
        s3_key (str): The S3 key to use when writing the time dimension table.
        process_list (List): List of column names.

    Returns:
        None.
    """
    # Process time argument here
    time_dim = (
        df
        .withColumn("weekend", dayofweek("date").isin([1, 7]).cast("int"))
        .withColumn("year", year("date"))
        .withColumn("month", month("date"))
        .withColumn("quarter", quarter("date"))
        .withColumn("weekofyear", weekofyear("date"))
        .dropDuplicates(column_list)
        .select(column_list)
    )

    logging.warning(f"Length of time_dimension is {time_dim.count()}")
    logging.warning(f"Schema is : \n{time_dim.printSchema()}")
    
    # Write the time dimension table to S3
    time_dim.write.parquet(f"s3a://{s3_bucket}/{s3_key}/time_table.parquet", mode='overwrite')


def process_orders(df: pyspark.sql.dataframe.DataFrame, s3_bucket: str, s3_key: str, column_list: List[str]) -> None:
    """
    Processes a date DataFrame to create a orders fact table and writes it to S3.

    Args:
        df (DataFrame): PySpark dataframe containing orders data.
        s3_bucket (str): The name of the S3 bucket to write the date dimension table to.
        s3_key (str): The S3 key to use when writing the time dimension table.
        process_list (List): List of column names.

    Returns:
        None.
    """
    order_fact = df \
            .withColumn("sale",
                        regexp_replace(col("sale"), "[^0-9.]", "")) \
            .withColumn("sale_dollar", col("sale").cast("double")) \
            .select(column_list)
    logging.warning(f"Length of fact category is : {order_fact.count()}")
    logging.warning(f"Schema is : \n{order_fact.printSchema()}")
    order_fact.write.parquet(f"s3a://{s3_bucket}/{s3_key}/order_fact.parquet",mode='overwrite')

def process_rest(df: pyspark.sql.dataframe.DataFrame, s3_bucket: str, s3_key: str, process_key: str, process_list: List[str]) -> None:
    """
    This function takes a Spark DataFrame, an S3 bucket name, an S3 key, a process key, and a list of columns to select.
    It selects unique rows based on the process_list and writes the resulting DataFrame to a parquet file in the S3 bucket
    with the name 'process_key_table.parquet'.

    Args:
        df (pyspark.sql.DataFrame): Input Spark DataFrame.
        s3_bucket (str): Name of the S3 bucket where the parquet file will be written.
        s3_key (str): S3 key path where the parquet file will be written.
        process_key (str): Process key to be added to the table name.
        process_list (list): List of column names to select unique rows based on.

    Returns:
        None
    """
    part_key = process_key.lower() + "_table" + ".parquet"
    inner_df = df.drop_duplicates(process_list).select(process_list)
    logging.warning(f"Length of dimension {part_key} is : {inner_df.count()}")
    logging.warning(f"Schema is : \n{inner_df.printSchema()}")
    inner_df.write.parquet(f"s3a://{s3_bucket}/{s3_key}/{part_key}", mode='overwrite')

