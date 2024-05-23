# Databricks notebook source
df=spark.read.option("header",True).option("inferSchema",True).csv("/FileStore/csv/businessFile.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC -- Change Column Name
# MAGIC -- Data Value and Units in same column
# MAGIC -- Add a new Column (Current Time Stamp)
# MAGIC -- Drop Column Series Title 5

# COMMAND ----------

df2=df.withColumnRenamed("Series_reference","series_references")

# COMMAND ----------

display(df2)

# COMMAND ----------

from pyspark.sql.functions import *
df3=df2.withColumn("DataValueUnits",concat("Data_value",lit(" - "),"UNITS")).drop("Data_value","UNITS","Series_title_5")

# COMMAND ----------

display(df3)

# COMMAND ----------

df4=df3.withColumn("current_time",current_timestamp())

# COMMAND ----------

display(df4)

# COMMAND ----------

df4.write.option("Header",True).mode("overwrite").csv("/FileStore/csv/output")

# COMMAND ----------

display(spark.read.csv("/FileStore/csv/output/part-00000-tid-7704981531040127206-11c9d330-e458-4fb4-ae23-01fe416862da-15-1-c000.csv"))
