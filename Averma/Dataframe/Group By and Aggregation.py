# Databricks notebook source
myData=[("James","Sales","NY",90000,34,10000),
        ("Michael","Sales","NY",86000,56,20000),
        ("Robert","Sales","CA",81000,30,23000),
        ("Maria","Finance","CA",90000,24,23000),
        ("Raman","Finance","CA",99000,40,24000),
        ("Scott","Finance","NY",83000,36,19000),
        ("Jenn","Finance","NY",79000,53,15000),
        ("Jeff","Marketing","CA",80000,25,18000),
        ("Kumar","Marketing","NY",91000,50,21000)
        ]
schema=["employee_name","department","state","salary","bonus"]
df=spark.createDataFrame(data=myData,schema=schema)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **show me count of department and group them**

# COMMAND ----------

display(df.groupBy("department").count())

# COMMAND ----------

display(df.groupBy("department").sum("Salary"))

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

display(df.groupBy("department")
        .agg(sum("salary").alias("Sum_Salary"),
             avg("salary").alias("avg_Salary")))
