import pandas as pd
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType

# Create a spark session
spark = SparkSession.builder \
    .appName("Spark sql") \
    .config("spark.executor.cores", "2") \
    .config("spark.driver.memory", "2g")\
    .config("spark.sql.shuffle.partitions", "2")\
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel('ERROR')

mtcars = pd.read_csv(
    'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/labs/data/mtcars.csv')
mtcars.rename(columns={'Unnamed: 0': 'name'}, inplace=True)
# print(mtcars.head())
sdf = spark.createDataFrame(mtcars)

# Register the Data Frame as a SQL temporary view
sdf.createTempView("cars")

# Select all
spark.sql("select * from cars").show()
# Select a column
spark.sql("select mpg from cars").show(5)
# Filtering
spark.sql("select * from cars where mpg > 20 and cyl < 6").show(5)
# Aggregation
spark.sql("select cyl, count(*) from cars group by cyl order by count(*) desc").show(5)


# Pandas User Defined Functions (UDF) built on top of Apache Arrow bring the best of both worlds,
# the ability to define low-overhead, high performance UDFs entirely in Python
# Build Scala Pandas UDF to convert wt column from imperial units into metric tons

@pandas_udf("float")
def convert_wt(s: pd.Series) -> pd.Series:
    # the formula to convert imperial to metrics
    return s * 0.45


spark.udf.register("convert_weight", convert_wt)

spark.sql("select *, wt as weight_imperial, convert_weight(wt) as weight_metric from cars").show()

# Display all cars with name merc
spark.sql("select * from cars where lower(name) like 'merc%'").show()


# Convert mpg to kmpl
@pandas_udf("float")
def convert_mpg(s: pd.Series) -> pd.Series:
    # the formula to convert mpg to kmpl
    return s * 0.425


spark.udf.register("convert_mpg", convert_mpg)
spark.sql("select *, mpg, convert_mpg(mpg) as kmpl from cars").show()