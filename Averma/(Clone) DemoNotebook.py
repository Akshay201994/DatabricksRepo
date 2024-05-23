# Databricks notebook source
a=20
b=23
if(a<b):
    print(a,'is smaller than',b)
else:
    print(b,'is smaller than',a)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'This is my first Notebook'

# COMMAND ----------

# MAGIC %md
# MAGIC # Title 1
# MAGIC ## title 2
# MAGIC ### Title 3
# MAGIC
# MAGIC text with **bold** and *italic* in it.
# MAGIC
# MAGIC Ordered list
# MAGIC 1. once
# MAGIC 2. two
# MAGIC 3. three
# MAGIC
# MAGIC Unordered list
# MAGIC * apples
# MAGIC * peaches
# MAGIC * bananas
# MAGIC
# MAGIC tables:
# MAGIC | user_id | user_name |
# MAGIC |---------|-----------|
# MAGIC |    1    |   Akshay  |
# MAGIC |    2    |   Rohit   |
# MAGIC |    3    |   Raj     |

# COMMAND ----------

# MAGIC %run ./includes/Setup

# COMMAND ----------

print(full_name)

# COMMAND ----------

# MAGIC %fs ls '/databricks-datasets'

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

files=dbutils.fs.ls('/databricks-datasets')
print(files)

# COMMAND ----------

display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC Create schema av2094

# COMMAND ----------

# MAGIC %sql
# MAGIC create table
# MAGIC av2094.email

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO av2094.email
# MAGIC FROM '/FileStore/csv/'
# MAGIC FILEFORMAT=CSV
# MAGIC FORMAT_OPTIONS ('header'='true')
# MAGIC COPY_OPTIONS ('mergeSchema'='True')
