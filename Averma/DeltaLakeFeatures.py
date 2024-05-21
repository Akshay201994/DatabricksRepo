# Databricks notebook source
# MAGIC %sql
# MAGIC DESCRIBE HISTORY employee

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM employee VERSION AS OF 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM employee @v2

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM employee

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM employee

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY employee

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE employee  TO VERSION AS OF 3

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM employee

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY employee

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL employee

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE employee
# MAGIC ZORDER BY id

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL employee

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY employee

# COMMAND ----------

# MAGIC %fs ls "dbfs:/user/hive/warehouse/employee"

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM employee

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employee'

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM employee RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enable= false;

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM employee RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE employee
