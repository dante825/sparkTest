# Testing AWS code whisperer
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Python Spark SQL basic example")\
    .config("spark.driver.cores", "2")\
    .getOrCreate()



def square(x):
    return x * x

