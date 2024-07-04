# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE countries
# MAGIC USING org.apache.spark.sql.jdbc
# MAGIC OPTIONS (
# MAGIC   url "jdbc:postgresql://us-east-1.1e7bb9df-f861-4305-85e1-d3c03984cbf7.aws.ybdb.io:5433/hr",
# MAGIC   dbtable "public.countries",
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
# MAGIC CREATE TABLE departments
# MAGIC USING org.apache.spark.sql.jdbc
# MAGIC OPTIONS (
# MAGIC   url "jdbc:postgresql://us-east-1.1e7bb9df-f861-4305-85e1-d3c03984cbf7.aws.ybdb.io:5433/hr",
# MAGIC   dbtable "public.departments",
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
# MAGIC CREATE TABLE employees
# MAGIC USING org.apache.spark.sql.jdbc
# MAGIC OPTIONS (
# MAGIC   url "jdbc:postgresql://us-east-1.1e7bb9df-f861-4305-85e1-d3c03984cbf7.aws.ybdb.io:5433/hr",
# MAGIC   dbtable "public.employees",
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
# MAGIC CREATE TABLE countries
# MAGIC USING org.apache.spark.sql.jdbc
# MAGIC OPTIONS (
# MAGIC   url "jdbc:postgresql://us-east-1.1e7bb9df-f861-4305-85e1-d3c03984cbf7.aws.ybdb.io:5433/hr",
# MAGIC   dbtable "public.countries",
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
# MAGIC CREATE TABLE job_history
# MAGIC USING org.apache.spark.sql.jdbc
# MAGIC OPTIONS (
# MAGIC   url "jdbc:postgresql://us-east-1.1e7bb9df-f861-4305-85e1-d3c03984cbf7.aws.ybdb.io:5433/hr",
# MAGIC   dbtable "public.job_history",
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
# MAGIC CREATE TABLE jobs
# MAGIC USING org.apache.spark.sql.jdbc
# MAGIC OPTIONS (
# MAGIC   url "jdbc:postgresql://us-east-1.1e7bb9df-f861-4305-85e1-d3c03984cbf7.aws.ybdb.io:5433/hr",
# MAGIC   dbtable "public.jobs",
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
# MAGIC CREATE TABLE locations
# MAGIC USING org.apache.spark.sql.jdbc
# MAGIC OPTIONS (
# MAGIC   url "jdbc:postgresql://us-east-1.1e7bb9df-f861-4305-85e1-d3c03984cbf7.aws.ybdb.io:5433/hr",
# MAGIC   dbtable "public.locations",
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
# MAGIC CREATE TABLE regions
# MAGIC USING org.apache.spark.sql.jdbc
# MAGIC OPTIONS (
# MAGIC   url "jdbc:postgresql://us-east-1.1e7bb9df-f861-4305-85e1-d3c03984cbf7.aws.ybdb.io:5433/hr",
# MAGIC   dbtable "public.regions",
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
# MAGIC SELECT * FROM countries;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   l.location_id
# MAGIC   ,r.region_id
# MAGIC   ,r.region_name
# MAGIC   ,c.country_id
# MAGIC   ,c.country_name
# MAGIC   ,l.state_province
# MAGIC   ,l.street_address
# MAGIC   ,l.postal_code
# MAGIC   ,d.department_id
# MAGIC   ,d.department_name
# MAGIC   ,d.manager_id
# MAGIC FROM regions r
# MAGIC   INNER JOIN countries c 
# MAGIC     ON c.region_id = r.region_id
# MAGIC   INNER JOIN locations l 
# MAGIC     ON l.country_id = c.country_id
# MAGIC   INNER JOIN departments d 
# MAGIC     ON d.location_id = l.location_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   e.employee_id
# MAGIC   ,e.first_name AS e_firstname
# MAGIC   ,e.last_name AS e_lastname
# MAGIC   ,e.manager_id
# MAGIC   ,m.first_name AS m_firstname
# MAGIC   ,m.last_name AS m_lastname
# MAGIC FROM employees e
# MAGIC     JOIN employees m
# MAGIC       ON e.manager_id = m.employee_id
# MAGIC ;

# COMMAND ----------

df_employee_manager = _sqldf
display(df_employee_manager)

# COMMAND ----------

df_employee_manager.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://us-east-1.1e7bb9df-f861-4305-85e1-d3c03984cbf7.aws.ybdb.io:5433/hr") \
    .option("dbtable", "public.employee_manager") \
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
# MAGIC -- According to job wise avg salary
# MAGIC SELECT e.job_id,j.job_title,avg(salary)
# MAGIC FROM
# MAGIC   employees e
# MAGIC     INNER JOIN jobs j
# MAGIC       ON e.job_id = j.job_id
# MAGIC GROUP BY e.job_id,j.job_title
# MAGIC ORDER BY j.job_title;

# COMMAND ----------

df_salary_job = _sqldf

df_salary_job.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://us-east-1.1e7bb9df-f861-4305-85e1-d3c03984cbf7.aws.ybdb.io:5433/hr") \
    .option("dbtable", "public.salary_job") \
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
# MAGIC SELECT
# MAGIC    r.region_id
# MAGIC   ,r.region_name
# MAGIC   ,SUM(e.salary) AS total_salary
# MAGIC FROM regions r
# MAGIC   INNER JOIN countries c 
# MAGIC     ON c.region_id = r.region_id
# MAGIC   INNER JOIN locations l 
# MAGIC     ON l.country_id = c.country_id
# MAGIC   INNER JOIN departments d 
# MAGIC     ON d.location_id = l.location_id
# MAGIC   INNER JOIN employees e 
# MAGIC     ON e.department_id = d.department_id
# MAGIC GROUP BY 
# MAGIC    r.region_id
# MAGIC   ,r.region_name

# COMMAND ----------

df_region_salary = _sqldf

df_region_salary.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://us-east-1.1e7bb9df-f861-4305-85e1-d3c03984cbf7.aws.ybdb.io:5433/hr") \
    .option("dbtable", "public.region_salary") \
    .option("user", "admin") \
    .option("driver", "org.postgresql.Driver") \
    .option("password", "Admin@123") \
    .option("useSSL", "true") \
    .option("ssl", "true") \
    .option("sslmode", "require") \
    .option("sslrootcert", "dbfs:/FileStore/yb_ssl/root__2_.crt") \
    .mode('overwrite').save()
