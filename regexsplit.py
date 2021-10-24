import os
import sys
from typing import Collection
from pyspark import SparkConf,SparkContext
from pyspark import sql
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.functions import col, column, explode, expr, regexp_replace, split, substring, when
import pyspark
spark = SparkSession.builder \
        .master("local") \
        .appName("App Name") \
        .getOrCreate() 
       
df=spark.read.option("multiline","true").json("file:///pysparkdemo/demonew.json")
responses=df.select(explode("data.results.user.location"))
responses=responses.select("col.*").alias("responses")
responses=responses.filter(col("city")=="lincoln")
# responses.show()
a=responses.select(col("responses.city"),col("responses.description"))
# a.show()
df1=a.select("responses.description",regexp_replace(col("responses.description"),'<br/>|<br>|<b>|</b>|</br>',"").alias("replaced"))

df2=df1.select("replaced",regexp_replace(col("replaced"),'criteria|accesscontrol|national',"-").alias("replacedone"))
df2.show()
df3=df2.withColumn('details',split("replacedone",'-').getItem(0))\
       .withColumn('details1',split("replacedone",'-').getItem(1))\
       .withColumn('details2',split("replacedone",'-').getItem(2))\
       .withColumn('details3',split("replacedone",'-').getItem(3))
df3.show()
df3.write.format("txt").mode("overwrite").save("E:\pysparkdemo\file.txt")
# df1.show()
# df1.printSchema()
# df2=df1.withColumn('details',split("replaced",'criteria').getItem(0))\
#     .withColumn('criteria',split("replaced",'criteria').getItem(1)) \
#     .withColumn('accesscontrol',split("replaced",'accesscontrol').getItem(1)) \
#     .withColumn('national',split("replaced",'national').getItem(1))
# df2.show()

# df3 = df1.withColumn('replaced',split('replaced', 'criteria')).withColumn('replaced',col('replaced')[1]).withColumn('replaced',col('replaced')[0])
# df2=df1.withColumn('details',split("replaced",'criteria').between('criteria','accesscontrol'))   
# df2.show()

