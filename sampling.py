from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType


spark = SparkSession.builder.appName("Smart-home-IoT").getOrCreate()
df = spark.read.parquet(r"C:\Users\mithu\Downloads\archive(2)\HomeC.parquet")
df = df.withColumn("generates_heat", col("furnace_1") + col("furnace_2"))
df = df.withColumn("kitchen", col("kitchen_12") + col("kitchen_14") + col("kitchen_38"))
df = df.withColumn("consumes_heat", col("fridge") + col("wine_cellar"))
df = df.withColumn("does_not_depend_on_weather", col("dishwasher") + col("barn") +
                   col("home_office") + col("kitchen") + col("living_room") + col("garage_door") + col("microwave"))
df = df.withColumn("temperature", (col("temperature") - 32) / 1.8)
window_spec = Window.orderBy("time")
df = df.withColumn("day", (((row_number().over(window_spec) - 1) / 1440) + 1).cast(IntegerType()))
win_spec = window_spec.partitionBy("day")
df = df.withColumn("dn_key", row_number().over(win_spec))
df.createOrReplaceTempView("table")
df = spark.sql("""select *,
 case when dn_key > 419 and dn_key < 1140 then 'D' else 'N' end as dn
 from table""")
df = df.groupBy(col("day"), col("dn")).agg(sum("use"), sum("gen"), avg("temperature"),
                      sum("generates_heat"), sum("consumes_heat"), sum("does_not_depend_on_weather").alias("stable"))
# df.cache()
nightDF = df.filter(col("dn") == 'N')
nightDF.write.csv(r"C:\Users\mithu\Downloads\testing\night.csv")
dayDF = df.filter(col("dn") == 'D')
dayDF.write.csv(r"C:\Users\mithu\Downloads\testing\day.csv")

