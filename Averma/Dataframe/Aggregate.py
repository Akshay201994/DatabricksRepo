# Databricks notebook source
# MAGIC %md
# MAGIC ### Count, Limit
# MAGIC **1. To check total number of rows.**
# MAGIC
# MAGIC ```
# MAGIC df.count()
# MAGIC ```
# MAGIC
# MAGIC **2. To check all columns.**
# MAGIC
# MAGIC ```
# MAGIC df.columns
# MAGIC ```
# MAGIC
# MAGIC **3. To check total number of Columns. We can use len function.**
# MAGIC
# MAGIC ```
# MAGIC len(df.columns)
# MAGIC ```
# MAGIC
# MAGIC **4. To use Limit function.**
# MAGIC
# MAGIC ```
# MAGIC df.limit(number)
# MAGIC ```

# COMMAND ----------

df=spark.read.option("Header",True).option("inferSchema",True).csv('dbfs:/databricks-datasets/online_retail/data-001/data.csv')
display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

df.columns

# COMMAND ----------

len(df.columns)

# COMMAND ----------

display(df.limit(2))
