# Databricks notebook source
# MAGIC %md
# MAGIC # Drop Function
# MAGIC
# MAGIC ```
# MAGIC dataframe.drop("ColumnName") -- in String
# MAGIC
# MAGIC dataframe.drop(col("columnName")) -- using col in sql function lib
# MAGIC ```

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/online_retail/data-001/data.csv

# COMMAND ----------

df=spark.read.option("Header",True).option("inferSchema",True).csv("dbfs:/databricks-datasets/online_retail/data-001/data.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

help(df.drop)

# COMMAND ----------

newDF=df.drop("Description","UnitPrice")

# COMMAND ----------

display(newDF)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df2=newDF.drop(col("StockCode"))

# COMMAND ----------

display(df2)
