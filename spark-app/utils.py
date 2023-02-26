from dataclasses import dataclass
from pyspark.sql.types import FloatType, StringType, IntegerType, DateType, StructField, StructType
from file_processor import *
from dataclasses import dataclass


def get_struct_types():
    return StructType([StructField("invoice_number", StringType(), True),
                     StructField("date", DateType(), False),
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

def get_dimension_lookup():
    return {
        "store": ["store_number", "store_name", "address", "store_location"],
        "item": [
            "item_number",
            "item_description"
        ],
        "vendor": ["vendor_number", "vendor_name"],
        "city": ["zip_code", "city"],
        "category": ["category", "category_name"],
        "item_price": ["item_number","date","state_bottle_cost","state_bottle_retail"],
        "date" : ["date"],
        "orders" : []
    }

