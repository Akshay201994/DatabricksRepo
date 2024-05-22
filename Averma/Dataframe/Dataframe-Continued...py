# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### With Column Function
# MAGIC
# MAGIC ** We can use display(df.WithColumn("NewColumnName",col("columnName")expression(+,-,* or any change we want))) **
# MAGIC
# MAGIC ### With Column Rename
# MAGIC
# MAGIC **is used to rename column. We can use Select function as well to provide new alias to column.**
# MAGIC ```
# MAGIC Rename Column Name- df.withColumnRenamed("oldColName","newColName")
# MAGIC ```

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/online_retail/data-001/

# COMMAND ----------

df=spark.read.option("Header",True).option("inferSchema",True).csv('dbfs:/databricks-datasets/online_retail/data-001/data.csv')
display(df)

# COMMAND ----------

from pyspark.sql.functions import col
display(df.withColumn("addedQuantity",col("Quantity")+1))

# COMMAND ----------

display(df.withColumnRenamed("InvoiceNo","InvoiceNumber"))
