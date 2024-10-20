from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
from datetime import date, timedelta
from libs.log import notification_discord

spark = SparkSession \
    .builder \
    .appName("klaus_sessions") \
    .getOrCreate()

df_silver = spark.read.json("/opt/airflow/bronze_layer",multiLine=True)

record_count = df_silver.count()

if record_count > 0:
    print("Dataframe has data, processing...")
else:
    message = "Dataframe is empty, notifying data engineering team..."
    notification_discord(message)
    sys.exit()

df_silver = df_silver.select(col('id'),
                             col('name'),
                             col('brewery_type'),  
                             col('phone'),
                             col('country'),
                             col('state'),
                             col('state_province'),
                             col('city'),
                             col('postal_code'),
                             col('latitude'),
                             col('longitude'),
                             col('address_1'),
                             col('address_2'),
                             col('address_3')
                             )

df_silver = df_silver \
    .withColumnRenamed("id", "id_brewery") \
    .withColumnRenamed("name", "name_brewery") \
    .withColumnRenamed("brewery_type", "type_brewery") \
    .withColumnRenamed("address_1", "address_1_brewery") \
    .withColumnRenamed("address_2", "address_2_brewery") \
    .withColumnRenamed("address_3", "address_3_brewery") \
    .withColumnRenamed("street", "street_brewery") \
    .withColumnRenamed("city", "city_brewery") \
    .withColumnRenamed("country", "country_brewery") \
    .withColumnRenamed("state", "state_brewery") \
    .withColumnRenamed("state_province", "state_province_brewery") \
    .withColumnRenamed("phone", "phone_brewery") \
    .withColumnRenamed("latitude", "latitude_brewery") \
    .withColumnRenamed("longitude", "longitude_brewery") \
    .withColumnRenamed("postal_code", "postal_code_brewery")



# if df.filter(col("name_state").isNull() | col("brewery_type").isNull()).count() > 0:
#     message = "The silver_brewery table has null values, informing the data engineering team..."
#     notification_discord(message)
#     print("Has null values")
#     sys.exit()
# else:
#     print("Has no null values")
df_silver = df_silver.withColumn("phone_brewery", when(col("phone_brewery").isNull(), "00000000000").otherwise(col("phone_brewery")))


#Add column for CDC control
df_silver = df_silver.withColumn("updated_at", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))

#Writing DataFrame
df_silver.write \
.format("parquet") \
.mode("append") \
.partitionBy("state_brewery") \
.option("mergeSchema", "true") \
.save(f'/opt/airflow/silver_layer/')
