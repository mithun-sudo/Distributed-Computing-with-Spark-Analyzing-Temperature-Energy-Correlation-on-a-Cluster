from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Creating a spark session.
spark = SparkSession.builder.appName("Smart-home-IoT").getOrCreate()
# Reading the file from hdfs.
df = spark.read.parquet("hdfs://ec2-34-221-187-35.us-west-2.compute.amazonaws.com:9000:/user/HomeC.parquet")
# Adding the columns based on its classifications.
df = df.withColumn("generates_heat", col("furnace_1") + col("furnace_2"))
df = df.withColumn("kitchen", col("kitchen_12") + col("kitchen_14") + col("kitchen_38"))
df = df.withColumn("consumes_heat", col("fridge") + col("wine_cellar"))
df = df.withColumn("does_not_depend_on_weather", col("dishwasher") + col("barn") + col("home_office") + col("kitchen") + col("living_room") + col("garage_door") + col("microwave"))
# Converting temperture from Fahrenheit to Celcius.
df = df.withColumn("temperature", (col("temperature") - 32) / 1.8)
# Creating a column day that denotes the day to which the record belongs.
window_spec = Window.orderBy("time")
df = df.withColumn("day", (((row_number().over(window_spec) - 1) / 1440) + 1).cast(IntegerType()))
# Creating a column dn_key that denotes minute to which the record belongs.
win_spec = window_spec.partitionBy("day")
df = df.withColumn("dn_key", row_number().over(win_spec))
# Creating a temporary table to use spark sql. The minutes are classified either as D or N.
df.createOrReplaceTempView("table")
df = spark.sql("""select *,
 case when dn_key > 419 and dn_key < 1140 then 'D' else 'N' end as dn
 from table""")
# Grouping the records by day, dn and performing aggregations on columns.
df = df.groupBy(col("day"), col("dn")).agg(sum("use"), sum("gen"), avg("temperature"),
                      sum("generates_heat"), sum("consumes_heat"), sum("does_not_depend_on_weather").alias("stable"))
# Dataframe is cached to improve the performance of the spark application.
df.cache()
# Filtering the records and writing it in hdfs
nightDF = df.filter(col("dn") == 'N')
nightDF.write.csv("hdfs://ec2-34-221-187-35.us-west-2.compute.amazonaws.com:9000:/user/night.csv")
dayDF = df.filter(col("dn") == 'D')
dayDF.write.csv("hdfs://ec2-34-221-187-35.us-west-2.compute.amazonaws.com:9000:/user/day.csv")

