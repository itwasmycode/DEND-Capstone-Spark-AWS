from dataclasses import dataclass
from function_lookup import *
import pyspark

@dataclass
class FileParser:
    data: dict
    df: pyspark.sql.dataframe.DataFrame
    s3_bucket: str
    s3_key: str
    
    def parse(self):
        """
        Loops through the data, applies the appropriate lookup process function to the DataFrame,
        and updates the DataFrame with the processed data.
        """
        
        lookup_process = {
            "item_price": process_item_price,
            "category": process_category,
            "orders": process_orders,
            "date": process_date,
            "store": process_rest,
            "item" : process_rest,
            "vendor": process_rest,
            "city": process_rest
        }
        diff_behaviour_funcs = ["item_price","category","orders","date"] 
        for key, val in self.data.items():
            if key not in diff_behaviour_funcs:
                lookup_process.get(key)(self.df,self.s3_bucket,self.s3_key,key,val)
            else:    
                lookup_process.get(key)(self.df,self.s3_bucket,self.s3_key,val)
        