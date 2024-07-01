# Databricks notebook source
dbutils.fs.rm('dbfs:/user/hive/warehouse/app_airline', True)

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE IF EXISTS  hive_metastore.app_Airline;
# MAGIC CREATE OR REPLACE TABLE app_Airline(
# MAGIC 	Unique_ID STRING,
# MAGIC 	Date_Created TIMESTAMP ,
# MAGIC 	Date_Modified TIMESTAMP,
# MAGIC 	Date_Deleted TIMESTAMP,
# MAGIC 	Sequence_Number INTEGER,
# MAGIC 	Airline_Name INT
# MAGIC )

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

# Create a Spark session
spark = SparkSession.builder.appName("Schema Example").getOrCreate()

# Define the schema
schema = StructType([
    StructField("Unique_ID", StringType(), True),
    StructField("Date_Created", TimestampType(), True),
    StructField("Date_Modified", TimestampType(), True),
    StructField("Date_Deleted", TimestampType(), True),
    StructField("Sequence_Number", IntegerType(), True),
    StructField("Airline_Name", IntegerType(), True)
])

# Create an empty DataFrame with the defined schema
df = spark.createDataFrame([], schema)

# Show the schema
df.printSchema()

# COMMAND ----------

df = spark.read.format('csv').schema(schema).option('header','true').load('dbfs:/FileStore/ipl_till_2017/Airline_1.csv')

# COMMAND ----------

df.write.mode('overwrite').insertInto('App_Airline')

# COMMAND ----------

# MAGIC %sql
# MAGIC CACHE SELECT * FROM App_Airline

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM App_Airline

# COMMAND ----------

df = spark.createDataFrame([
    ['red', 'banana', 1, 10], ['blue', 'banana', 2, 20], ['red', 'carrot', 3, 30],
    ['blue', 'grape', 4, 40], ['red', 'carrot', 5, 50], ['black', 'carrot', 6, 60],
    ['red', 'banana', 7, 70], ['red', 'grape', 8, 80]], schema=['color', 'fruit', 'v1', 'v2'])
df.show()

df.groupby('color').avg().show()

df.dtypes