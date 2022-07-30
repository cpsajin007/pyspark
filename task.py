import os
import sys
from typing import Collection
from pyspark import SparkConf,SparkContext
from pyspark import sql
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.functions import col, column, explode
import pyspark
spark = SparkSession.builder \
        .master("local") \
        .appName("App Name") \
        .getOrCreate() 


print("hiii");
df=spark.read.option("multiline","true").json("file:///pysparkdemo/demonew.json")

# df.show()
# details = df.withColumn("city",sf.explode(sf.col("data.results.user.location.city")))
# details.show()
# street = df.withColumn("street", sf.explode(sf.col("data.results.user.location.street")))
# street.show()
df.show()


# original code
location= df.select(explode("data.results.user.location").alias("location"))
location.show()

x= location.select(col("location.city").alias("city"),
col("location.street").alias("street"),
col("location.state").alias("state"))
x.show()
x.filter("location.city LIKE 'lincoln'").show()

# df2=df.select(col("data.results.user.location.city"),
# col("data.results.user.location.street"),
# col("data.results.user.location.state")
# ).show(truncate=False)






# sajin = df.withColumn("street", sf.explode(sf.col("data.results.user.location.street"))) \
#   .withColumn("City", sf.explode(sf.col("data.results.user.location.city"))) \
#   .withColumn("state",sf.explode(sf.col("data.results.user.location.state"))) \
#   .distinct()
# sajin.show()
