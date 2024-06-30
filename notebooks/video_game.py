# Databricks notebook source
df = spark.read.csv('dbfs:/FileStore/vg_data_dictionary.csv',header=True)
df.show()

# COMMAND ----------

df1 = spark.read.csv('dbfs:/FileStore/vgchartz_2024.csv',header=True,inferSchema=True)
display(df1)

# COMMAND ----------

df_drop_null = df1.dropna(subset='last_update')

# COMMAND ----------

display(df_drop_null)

# COMMAND ----------

from pyspark.sql.functions import col,year,max
df_filter_year = df1.filter((year('release_date')=='2010'))
df_filter_max_sales=df_filter_year.select(max('total_sales'))
#df_filter = df1.filter((col('release_date')=='2013-09-24'))
#display(df_filter.select('title','genre','publisher','release_date').distinct())
display(df_filter_year.select('title','genre','publisher','release_date').distinct())

# COMMAND ----------

display(df1.select('title').distinct().count())


# COMMAND ----------

display(df1.select('console').distinct())

# COMMAND ----------

from pyspark.sql.functions import max
df1.select(max('release_date')).show()

# COMMAND ----------

