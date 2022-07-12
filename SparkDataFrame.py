import pandas as pd
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# Create a spark context class
sc = SparkContext()

# Create a spark session
spark = SparkSession.builder\
    .appName("Python Spark DataFrames basic example")\
    .config("spark.some.config.option", "some_value")\
    .getOrCreate()

mtcars = pd.read_csv('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/labs/data/mtcars.csv')
# print(mtcars.head())
sdf = spark.createDataFrame(mtcars)
sdf.printSchema()
sdf.show(5)
# Select a column
sdf.select("mpg").show(5)
# Filter the data
sdf.filter(sdf['mpg'] < 18).show(5)
# Create a new column, convert weight values from lb to metric ton
sdf.withColumn('wtTon', sdf['wt'] * 0.45).show(5)

# Filter and aggregations
# average weight of car by cylinders
sdf.groupby(['cyl']).agg({"wt": "AVG"}).show(5)

# count of cars group by cylinders, sort desc
sdf.groupby(['cyl']).agg({"wt": "COUNT"}).sort("count(wt)", ascending=False).show(5)

# cars that have at least 5 cylinders
sdf.filter(sdf['cyl'] > 5).show(5)

# mean weight of the cars in metric ton
sdf.withColumn('wtTon', sdf['wt'] * 0.45).agg({"wtTon": "AVG"}).show(5)

# change mpg (mile per gallon) to kmpl (km per litre)
sdf.withColumn('kmpl', sdf['mpg'] * 0.425).sort('kmpl', ascending=False).show(5)

