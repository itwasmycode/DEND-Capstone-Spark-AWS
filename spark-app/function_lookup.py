from pyspark.sql.functions import (
    col,
    dayofweek,
    year,
    quarter,
    month,
    weekofyear,
    regexp_replace,
)
import logging

def process_item_price(df, s3_bucket:str, s3_key: str, column_list:list):
    # process item price argument here
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

def process_category(df, s3_bucket:str, s3_key: str, column_list:list):
    # process category argument here
    inner_df = df.withColumn("Category",col("Category").cast('int'))
    inner_df = df.drop_duplicates(column_list).select(column_list)
    logging.warning(f"Length of dimension category is : {inner_df.count()}")
    inner_df.write.parquet(f"s3a://{s3_bucket}/{s3_key}/category_table.parquet",mode='overwrite')

def process_date(df,s3_bucket,s3_key, process_dict):
    # process time argument here
    time_dim = (
        df \
        .withColumn("weekend", dayofweek("date").isin([1, 7]).cast("int"))
        .withColumn("year", year("date"))
        .withColumn("month", month("date"))
        .withColumn("quarter", quarter("date"))
        .withColumn("weekofyear", weekofyear("date"))
        .dropDuplicates(["date_ex", "weekend", "year", "month", "quarter"])
        .select(["date", "weekend", "year", "month", "quarter"])
    )
    
    logging.warning(f"Length of time_dimension is {time_dim.count()}")
    logging.warning(f"Schema is : \n{time_dim.printSchema()}")
    time_dim.write.parquet(f"s3a://{s3_bucket}/{s3_key}/time_table.parquet",mode='overwrite')

def process_orders(df,s3_bucket,s3_key, process_dict):
    # process orders argument here
    order_fact = df \
            .withColumn("sale",
                        regexp_replace(col("sale"), "[^0-9.]", "")) \
            .withColumn("sale_dollar", col("sale").cast("double")) \
            .select(["invoice_number","date","store_number","zip_code","county_number","vendor_number","item_number","category_number","bottles_sold","sale_dollar","volume_sold_liters"])
    order_fact.printSchema()
    order_fact.write.parquet(f"s3a://{s3_bucket}/{s3_key}/order_fact.parquet",mode='overwrite')

def process_rest(df, s3_bucket, s3_key, process_key, process_list):
    # process rest argument here
    part_key = process_key.lower() + "_table"+".parquet"
    inner_df = df.drop_duplicates(process_list).select(process_list)
    logging.warning(f"Length of dimension {part_key} is : {inner_df.count()}")
    logging.warning(f"Schema is : \n{inner_df.printSchema()}")
    inner_df.write.parquet(f"s3a://{s3_bucket}/{s3_key}/{part_key}",mode='overwrite')

