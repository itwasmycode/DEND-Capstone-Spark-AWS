from pyspark.sql.types import FloatType, StringType, IntegerType, DateType, StructField, StructType
from file_processor import *
from typing import Dict, List

"""
This file defines the data schema and lookup dimensions for a retail sales dataset.

The get_struct_types() function returns the data schema for the retail sales dataset, which is a StructType containing 25 fields:

invoice_number: StringType
date: DateType
store_number: IntegerType
store_name: StringType
address: StringType
city: StringType
zip_code: StringType
store_location: StringType
county_number: IntegerType
county: StringType
category: IntegerType
category_name: StringType
vendor_number: IntegerType
vendor_name: StringType
item_number: IntegerType
item_description: StringType
pack: IntegerType
bottle_volume: StringType
state_bottle_cost: StringType
state_bottle_retail: StringType
bottles_sold: IntegerType
sale: StringType
volume_sold_liters: FloatType
volume_sold_gallons: FloatType
The get_dimension_lookup() function returns a dictionary of dimensions used for the retail sales dataset, which includes the following dimensions:

store: store_number, store_name, address, store_location
item: item_number, item_description
vendor: vendor_number, vendor_name
city: zip_code, city
category: category, category_name
item_price: item_number, date, state_bottle_cost, state_bottle_retail
date: date
orders: (empty list)
This file depends on the file_processor module.
"""

def get_struct_types() -> StructType:
    """
    Returns the schema for the retail sales dataset, which is a StructType containing 25 fields.
    """
    return StructType([
            StructField("invoice_number", StringType(), True),
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
            StructField("volume_sold_gallons", FloatType(), False)
            ])

def get_dimension_lookup() -> Dict[str, List[str]]:
    """
    Returns a dictionary of dimensions used for the retail sales dataset, including store, item, vendor, city,
    category, item_price, date, and orders.
    """
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
        "date" : ["date", "weekend", "year", "month", "quarter"],
        "orders" : ["invoice_number","date","store_number","zip_code","county_number","vendor_number","item_number","category_number","bottles_sold","sale_dollar","volume_sold_liters"]
    }

