  # SQL Table Script
  # CREATE TABLE terrascope.master
  # places_id integer
  # name string
  # country string
  # people_id integer
  # name string
  # age integer
  
  

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Terrascope Assessment") \
    .getOrCreate()
    
  
df_people=spark.read.format("csv") \
      .option("header", True) \
      .option("inferSchema",True)\
      .load("people.csv")
      
df_places=spark.read.format("csv") \
      .option("header", True) \
      .option("inferSchema",True)\
      .load("people.csv")
      
df_master=df_people.join(df_places,df_places.places_id===df_people.places_id)

df_master.write.format("parquet").mode("overwrite").insertInto("terrascope.master")

df_final=df_master.groupBy("country").agg(sum(lit("1"))

df_final.write.json('output.json')