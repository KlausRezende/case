from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *
import sys
from datetime import date, timedelta

spark = SparkSession \
    .builder \
    .appName("klaus_session") \
    .getOrCreate()

df = spark.read.parquet("/opt/airflow/silver/")

df = df.withColumn('index',row_number().over(Window.partitionBy(col("id_brewery")).orderBy(col("updated_at").desc())))
df = df.filter(df.index == 1) 
df = df.drop("index")

df_gold = df.groupBy(df.state_brewery, df.type_brewery).count()

df_gold = df_gold.withColumnRenamed("count", "breweries_quantity")

#Writing DataFrame
df_gold.write \
.format("parquet") \
.mode("append") \
.option("mergeSchema", "true") \
.save('/opt/airflow/gold/')