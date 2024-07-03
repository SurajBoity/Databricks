# Databricks notebook source
# read data from Yugbayte DB
df = spark.read \
   .format("jdbc") \
   .option("url", "jdbc:postgresql://us-east-1.1e7bb9df-f861-4305-85e1-d3c03984cbf7.aws.ybdb.io:5433/test") \
   .option("dbtable", "public.App_Airline_1") \
   .option("user", "admin") \
   .option("driver", "org.postgresql.Driver") \
   .option("password", "Admin@123") \
   .option("useSSL", "true") \
   .option("ssl", "true") \
   .option("sslmode", "require") \
   .option("sslrootcert", "dbfs:/FileStore/yb_ssl/root__2_.crt") \
   .load()

# COMMAND ----------

# read data from yugabytedb and write data into parquet file
df.write.mode('overwrite').parquet('dbfs:/FileStore/yb_hr/countries.parquet')

# COMMAND ----------

#display csv data
read_df = spark.read.format('parquet').load('dbfs:/FileStore/yb_hr/countries.parquet')
display(read_df)

# COMMAND ----------

from pyspark.sql.types import StringType,IntegerType,StructType,StructField,TimestampType

schema_app_airline = StructType([
    StructField("Unique_ID", StringType(), True),
    StructField("Date_Created", TimestampType(), True),
    StructField("Date_Modified", TimestampType(), True),
    StructField("Date_Deleted", TimestampType(), True),
    StructField("Sequence_Number", IntegerType(), True),
    StructField("Airline_Name", IntegerType(), True)
])

# COMMAND ----------

# Reading data from CSV file
df_csv = spark.read.csv('dbfs:/FileStore/testing/Airline_2.csv',header=True,schema=schema_app_airline)
df_csv.show()

# COMMAND ----------

# Write data into YugabyteDB table
df_csv.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://us-east-1.1e7bb9df-f861-4305-85e1-d3c03984cbf7.aws.ybdb.io:5433/test") \
    .option("dbtable", "public.App_Airline_1") \
    .option("user", "admin") \
    .option("driver", "org.postgresql.Driver") \
    .option("password", "Admin@123") \
    .option("useSSL", "true") \
    .option("ssl", "true") \
    .option("sslmode", "require") \
    .option("sslrootcert", "dbfs:/FileStore/yb_ssl/root__2_.crt") \
    .mode('overwrite').save()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE App_Airline
# MAGIC USING org.apache.spark.sql.jdbc
# MAGIC OPTIONS (
# MAGIC   url "jdbc:postgresql://us-east-1.1e7bb9df-f861-4305-85e1-d3c03984cbf7.aws.ybdb.io:5433/test",
# MAGIC   dbtable "public.App_Airline_1",
# MAGIC   user "admin",
# MAGIC   password "Admin@123",
# MAGIC   driver "org.postgresql.Driver",
# MAGIC   useSSL "true",
# MAGIC   ssl "true",
# MAGIC   sslmode "require",
# MAGIC   sslrootcert "dbfs:/FileStore/yb_ssl/root__2_.crt"
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM App_Airline;