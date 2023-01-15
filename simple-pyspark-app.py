from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Simple PySpark App")
sc = SparkContext(conf=conf)

# Create a RDD from a list
data = [1, 2, 3, 4, 5]
rdd = sc.parallelize(data)

# Perform a simple transformation
squared_rdd = rdd.map(lambda x: x*x)

# Collect the results and print them
results = squared_rdd.collect()
print(results)
