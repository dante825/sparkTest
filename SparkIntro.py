import findspark
import time
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# findspark.init()

sc = SparkContext()
spark = SparkSession \
    .builder \
    .appName("Python Spark Data Frames basic example") \
    .config("spark.some.config.option", "some_value") \
    .getOrCreate()

# Creating RDDs
data = range(1, 30)
xrangeRDD = sc.parallelize(data)
# print(xrangeRDD.collect())

# Transformation, transform the RDD
subRDD = xrangeRDD.map(lambda x: x - 1)
# print(subRDD.collect())
filteredRDD = subRDD.filter(lambda x: x < 10)
# print(filteredRDD.collect())

# Caching data
test = sc.parallelize(range(1,5000), 4)
test.cache()

t1 = time.time()
# first count would trigger evaluation of count *and* cache
count1 = test.count()
dt1 = time.time() - t1
print("dt1: ", dt1)

t2 = time.time()
# second count operates on cache data only
count2 = test.count()
dt2 = time.time() - t2
print("dt2: ", dt2)

# Spark SQL
df = spark.read.json("./input/people.json").cache()
df.show()
df.printSchema()

# Register the Data Frame as a SQL temporary view
df.createTempView("people")

# Different ways to select columns
df.select("name").show()
df.select(df["name"]).show()
spark.sql("Select name from people").show()

# Perform basic filtering
df.filter(df['age'] > 21).show()
spark.sql("select * from people where age > 21").show()

# Perform basic aggregation
df.groupBy("age").count().show()
spark.sql("select age, count(*) from people group by age").show()

# Create a RDD with integer range 1 - 50
# Apply a multiply every number by 2
test1 = range(1, 50)
test1RDD = sc.parallelize(test1)
test1RDD.cache()
print(test1RDD.collect())
test1RDD = test1RDD.map(lambda x: x*2)
print(test1RDD.collect())

# Read people2.json
# use SQL to determine the average age
test2df = spark.read.json("./input/people2.json")
test2df.show()
test2df.createTempView("people2")
spark.sql("select avg(age) from people2").show()
spark.sql("select sum(age)/count(*) from people2").show()

# Close the spark session
spark.stop()