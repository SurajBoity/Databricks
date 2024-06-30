# Databricks notebook source
# MAGIC %pip install openpyxl

# COMMAND ----------

import pyspark.pandas as pd
from pandas import ExcelFile
pdf = pd.read_excel('dbfs:/FileStore/Student.xlsx')
display(pdf.to_spark())