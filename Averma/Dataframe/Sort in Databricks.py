# Databricks notebook source
# MAGIC %fs ls dbfs:/databricks-datasets/online_retail/data-001/

# COMMAND ----------

df=spark.read.option("Header",True).option("inferSchema",True).csv("dbfs:/databricks-datasets/online_retail/data-001/data.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

display(df.sort("Quantity"))

# COMMAND ----------

# MAGIC %md
# MAGIC # We can use OrderBy as well in place of sort

# COMMAND ----------

display(df.orderBy("Quantity"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Sort in Descending order
# MAGIC
# MAGIC ```
# MAGIC df.sort/orderBy("ColName",ascending=False)
# MAGIC
# MAGIC df.sort/orderBy(col("ColumnName").desc())
# MAGIC ```

# COMMAND ----------

display(df.orderBy("Quantity",ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sort with Multiple Columns

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

display(df.sort("InvoiceNo",col("StockCode").desc()))
