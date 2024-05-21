# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE employee
# MAGIC (id INT,
# MAGIC fullName string,
# MAGIC salary double)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO employee
# MAGIC VALUES
# MAGIC (1,'AKshay Verma',15000),
# MAGIC (2,'Rohit Raj',18000),
# MAGIC (3,'Rahul SHarma',12000),
# MAGIC (4,'Bob Dylan',20000)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM employee

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL employee

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employee'

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE employee
# MAGIC SET salary=salary+100
# MAGIC WHERE fullName like 'R%'

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE employee
# MAGIC SET fullName='Akshay Verma'
# MAGIC WHERE id=1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM employee

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employee'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL employee

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY employee

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employee/_delta_log'

# COMMAND ----------

# MAGIC %fs head 'dbfs:/user/hive/warehouse/employee/_delta_log/00000000000000000002.json'

# COMMAND ----------


